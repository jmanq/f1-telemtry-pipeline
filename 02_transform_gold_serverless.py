# Databricks notebook source
from pyspark.sql import functions as F, Window as W

CAT, SCH = "workspace", "f1"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")

# load Bronze
laps  = spark.table(f"{CAT}.{SCH}.bronze_laps")
telem = spark.table(f"{CAT}.{SCH}.bronze_telemetry")


# silver tables

## ensure numeric seconds, keep stint, dedupe
laps_s = (
    laps
    .withColumn("LapSeconds", F.col("LapTime").cast("double"))
    .withColumn("S1", F.col("Sector1Time").cast("double"))
    .withColumn("S2", F.col("Sector2Time").cast("double"))
    .withColumn("S3", F.col("Sector3Time").cast("double"))
    .dropDuplicates(["Season","Round","SessionName","Driver","LapNumber"])
)

## clean & enrich
telem_s = (
    telem
    .withColumn("SpeedMS", F.col("Speed") / 3.6)
    .withColumn("BrakePct", F.col("Brake").cast("double"))
    .dropna(subset=["Distance", "Speed"])
)

## write silver (drop first to avoid schema conflicts)
spark.sql(f"DROP TABLE IF EXISTS {CAT}.{SCH}.silver_laps")
spark.sql(f"DROP TABLE IF EXISTS {CAT}.{SCH}.silver_telemetry")

laps_s.write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.{SCH}.silver_laps")
telem_s.write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.{SCH}.silver_telemetry")

print("Silver tables refreshed.")


# gold: Stints & Sectors


# stints: per-driver, per-stint, per-compound summaries
stints = (
    laps_s
    .groupBy("Season","Round","SessionName","Driver","Stint","Compound")
    .agg(
        F.count("*").alias("Laps"),
        F.avg("LapSeconds").alias("AvgLap"),
        F.min("LapSeconds").alias("BestLap"),
    )
)

## sectors: per-driver average sectors (session-level)
sectors = (
    laps_s
    .groupBy("Season","Round","SessionName","Driver")
    .agg(
        F.avg("S1").alias("AvgS1"),
        F.avg("S2").alias("AvgS2"),
        F.avg("S3").alias("AvgS3"),
        F.avg("LapSeconds").alias("AvgLap"),
    )
)

spark.sql(f"DROP TABLE IF EXISTS {CAT}.{SCH}.gold_stints")
spark.sql(f"DROP TABLE IF EXISTS {CAT}.{SCH}.gold_sectors")

stints.write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.{SCH}.gold_stints")
sectors.write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.{SCH}.gold_sectors")

print("Gold stints & sectors refreshed.")


# gold: Stint-level reliability


# drop any Stint/Compound coming from telemetry so only keep the canonical ones from laps
cols_to_drop = [c for c in ["Stint", "Compound"] if c in telem_s.columns]
telem_base = telem_s.drop(*cols_to_drop)

## join telemetry with laps to bring in stint & compound (from laps_s only)
telem_enriched = (
    telem_base.alias("t")
    .join(
        laps_s.select(
            "Season","Round","SessionName","Driver","LapNumber","Stint","Compound"
        ).alias("l"),
        ["Season","Round","SessionName","Driver","LapNumber"],
        "left"
    )
)

## d_dist within each Lap (keeping Stint in partition to be safe)
from pyspark.sql import Window as W

w = W.partitionBy("Season","Round","SessionName","Driver","Stint","LapNumber").orderBy("Distance")

dd = telem_enriched.withColumn(
    "d_dist",
    F.col("Distance") - F.lag("Distance").over(w)
)

## aggregate per (Season, Round, SessionName, Driver, Stint, Compound)
agg = (
    dd.groupBy("Season","Round","SessionName","Driver","Stint","Compound")
      .agg(
          F.count("*").alias("samples"),
          F.sum(F.when(F.col("SpeedMS").isNull(), 1).otherwise(0)).alias("null_speed"),
          F.sum(F.when(F.col("d_dist") <= 0, 1).otherwise(0)).alias("dup_dist_count"),
          # Large gaps: > 50m between samples (tuned for FastF1)
          F.sum(F.when(F.col("d_dist") > 50.0, 1).otherwise(0)).alias("gap_large_count"),
          F.avg("d_dist").alias("mean_ddist"),
          F.stddev("d_dist").alias("std_ddist"),
      )
      .withColumn(
          "non_null_speed_ratio",
          1 - F.col("null_speed") / F.col("samples")
      )
      .withColumn(
          "deltas_n",
          F.when(F.col("samples") > 1, F.col("samples") - 1).otherwise(F.lit(1))
      )
      .withColumn(
          "gap_rate",  # fraction of large gaps in the stint
          F.col("gap_large_count") / F.col("deltas_n")
      )
      .withColumn(
          "dup_ratio",
          F.col("dup_dist_count") / F.col("deltas_n")
      )
      .withColumn(
          "rate_drift",
          F.when(F.col("mean_ddist") > 0,
                 F.col("std_ddist") / F.col("mean_ddist")).otherwise(None)
      )
)

## scoring tuned for FastF1:
##  - distance_score: 1 when no large gaps, decreases with gap_rate
##  - drift_score: rate_drift ~0.5 is normal; >=1.5 considered "bad"
##  - speed_score: non-null speed ratio directly

rel_stint = (
    agg
    ## safety clamps
    .withColumn(
        "gap_rate_clamped",
        F.least(F.greatest(F.col("gap_rate"), F.lit(0.0)), F.lit(1.0))
    )
    .withColumn(
        "non_null_speed_ratio_clamped",
        F.least(F.greatest(F.col("non_null_speed_ratio"), F.lit(0.0)), F.lit(1.0))
    )
    .withColumn(
        "rate_drift_norm",
        F.least(F.coalesce(F.col("rate_drift"), F.lit(1.5)) / F.lit(1.5), F.lit(1.0))
    )
    .withColumn("distance_score", 1 - F.col("gap_rate_clamped"))
    .withColumn("drift_score", 1 - F.col("rate_drift_norm"))
    .withColumn("speed_score", F.col("non_null_speed_ratio_clamped"))
    .withColumn(
        "score",
        0.5*F.col("distance_score")
        + 0.3*F.col("drift_score")
        + 0.2*F.col("speed_score")
    )
    ## for compatibility with earlier naming: dropout_pct = large-gap rate
    .withColumn("dropout_pct", F.col("gap_rate_clamped"))
)

spark.sql(f"DROP TABLE IF EXISTS {CAT}.{SCH}.gold_reliability_stint")

rel_stint.write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.{SCH}.gold_reliability_stint")

print("Gold stint-level reliability refreshed.")



# stint + Reliability join


driver_rel_stint = (
    rel_stint.groupBy("Season","Round","SessionName","Driver","Stint","Compound")
             .agg(
                 F.avg("score").alias("RelScoreAvg"),
                 F.avg("dropout_pct").alias("DropoutPctAvg"),
                 F.avg("rate_drift").alias("RateDriftAvg"),
                 F.avg("non_null_speed_ratio").alias("NonNullSpeedRatioAvg"),
             )
)

spark.sql(f"DROP TABLE IF EXISTS {CAT}.{SCH}.gold_reliability_driver_stint")
driver_rel_stint.write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.{SCH}.gold_reliability_driver_stint")

## join to stints
joined = (
    stints.alias("s")
    .join(
        driver_rel_stint.alias("r"),
        ["Season","Round","SessionName","Driver","Stint","Compound"],
        "left"
    )
    .select(
        "s.Season","s.Round","s.SessionName","s.Driver","s.Stint","s.Compound","s.Laps",
        "s.AvgLap","s.BestLap",
        "r.RelScoreAvg","r.DropoutPctAvg","r.RateDriftAvg","r.NonNullSpeedRatioAvg"
    )
)

spark.sql(f"DROP TABLE IF EXISTS {CAT}.{SCH}.gold_stints_with_reliability")
joined.write.format("delta").mode("overwrite").saveAsTable(f"{CAT}.{SCH}.gold_stints_with_reliability")

print("Gold stints_with_reliability refreshed.")


# human-readable SQL views


## stints + reliability, formatted
spark.sql(f"""
CREATE OR REPLACE VIEW {CAT}.{SCH}.v_gold_stints_with_reliability_fmt AS
SELECT
  s.Season,
  s.Round,
  s.SessionName,
  s.Driver,
  s.Stint,
  UPPER(s.Compound)            AS Compound,
  s.Laps,

  -- raw numerics for sorting/math
  s.AvgLap                     AS AvgLap_s,
  s.BestLap                    AS BestLap_s,

  -- mm:ss.sss formatting
  format_string('%d:%06.3f',
                CAST(floor(s.AvgLap/60) AS INT),
                s.AvgLap - 60*floor(s.AvgLap/60)) AS AvgLap,
  format_string('%d:%06.3f',
                CAST(floor(s.BestLap/60) AS INT),
                s.BestLap - 60*floor(s.BestLap/60)) AS BestLap,

  r.RelScoreAvg,

  CASE
    WHEN r.RelScoreAvg IS NULL THEN '—'
    WHEN r.RelScoreAvg >= 0.90 THEN '🟢 Excellent'
    WHEN r.RelScoreAvg >= 0.80 THEN '🟢 Good'
    WHEN r.RelScoreAvg >= 0.65 THEN '🟡 Fair'
    WHEN r.RelScoreAvg >= 0.50 THEN '🟠 Low'
    ELSE '🔴 Poor'
  END AS Reliability,

  r.DropoutPctAvg              AS DropoutPct,
  CASE
    WHEN r.DropoutPctAvg IS NULL THEN '—'
    ELSE format_string('%.1f%%', 100.0 * r.DropoutPctAvg)
  END AS DropoutPctFmt,

  r.RateDriftAvg               AS RateDrift,
  ROUND(r.RateDriftAvg, 3)     AS RateDriftFmt,

  r.NonNullSpeedRatioAvg       AS SpeedDataRatio,
  CASE
    WHEN r.NonNullSpeedRatioAvg IS NULL THEN '—'
    ELSE format_string('%.1f%%', 100.0 * r.NonNullSpeedRatioAvg)
  END AS SpeedDataRatioFmt

FROM {CAT}.{SCH}.gold_stints s
LEFT JOIN {CAT}.{SCH}.gold_reliability_driver_stint r
  ON  s.Season      = r.Season
  AND s.Round       = r.Round
  AND s.SessionName = r.SessionName
  AND s.Driver      = r.Driver
  AND s.Stint       = r.Stint
  AND s.Compound    = r.Compound
""")

# sectors view
spark.sql(f"""
CREATE OR REPLACE VIEW {CAT}.{SCH}.v_gold_sectors_fmt AS
SELECT
  Season,
  Round,
  SessionName,
  Driver,

  AvgLap           AS AvgLap_s,
  AvgS1            AS S1_s,
  AvgS2            AS S2_s,
  AvgS3            AS S3_s,

  format_string('%d:%06.3f',
                CAST(floor(AvgLap/60) AS INT),
                AvgLap - 60*floor(AvgLap/60)) AS AvgLap,
  format_string('%.3f', AvgS1) AS S1,
  format_string('%.3f', AvgS2) AS S2,
  format_string('%.3f', AvgS3) AS S3

FROM {CAT}.{SCH}.gold_sectors
""")

# stint deltas vs best-in-compound (per session)
spark.sql(f"""
CREATE OR REPLACE VIEW {CAT}.{SCH}.v_gold_stints_deltas_fmt AS
WITH best AS (
  SELECT
    Season, Round, SessionName, Compound,
    MIN(BestLap) AS BestInClass_s
  FROM {CAT}.{SCH}.gold_stints
  GROUP BY Season, Round, SessionName, Compound
)
SELECT
  s.Season, s.Round, s.SessionName, s.Driver, s.Stint,
  UPPER(s.Compound) AS Compound, s.Laps,

  s.AvgLap AS AvgLap_s,
  format_string('%d:%06.3f',
                CAST(floor(s.AvgLap/60) AS INT),
                s.AvgLap - 60*floor(s.AvgLap/60)) AS AvgLap,

  s.BestLap AS BestLap_s,
  format_string('%d:%06.3f',
                CAST(floor(s.BestLap/60) AS INT),
                s.BestLap - 60*floor(s.BestLap/60)) AS BestLap,

  b.BestInClass_s,
  (s.BestLap - b.BestInClass_s) AS DeltaBestToClassLeader_s,
  format_string('%.3f', (s.BestLap - b.BestInClass_s)) AS DeltaBestToClassLeader

FROM {CAT}.{SCH}.gold_stints s
JOIN best b
  ON  s.Season      = b.Season
  AND s.Round       = b.Round
  AND s.SessionName = b.SessionName
  AND s.Compound    = b.Compound
""")

print("Silver/Gold + stint reliability + views refresh complete.")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Driver, Stint, Compound, Laps,
# MAGIC        AvgLap, BestLap,
# MAGIC        RelScoreAvg, Reliability, DropoutPctFmt, SpeedDataRatioFmt
# MAGIC FROM workspace.f1.v_gold_stints_with_reliability_fmt
# MAGIC WHERE Season = 2024 AND Round = 12 AND SessionName = 'Race'
# MAGIC ORDER BY Driver, Stint, Compound;