# Databricks notebook source
import pandas as pd
import fastf1 as ff1
from pyspark.sql import functions as F

CAT, SCH = "workspace", "f1"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")


# parameters from widgets

SEASON  = int(dbutils.widgets.get("season"))     
EVENT   = dbutils.widgets.get("event")           
SESSION = dbutils.widgets.get("session").lower() 

print("SESSION INFO:", SEASON, EVENT, SESSION)


# load session via FastF1

ff1.Cache.set_disabled()  ## no filesystem writes

ses = ff1.get_session(SEASON, EVENT, SESSION)
ses.load(laps=True, telemetry=True, weather=False, messages=False)

laps = ses.laps.reset_index(drop=True)

event_name = getattr(ses.event, "EventName",
                     getattr(ses.event, "OfficialEventName", "Unknown"))
round_no   = int(getattr(ses.event, "RoundNumber",
                          getattr(ses.event, "Round", -1)))
session_name = ses.name  

print("Resolved event:", event_name, "| round:", round_no, "| session:", session_name)


# laps (with stints)


time_cols = ["LapTime", "Sector1Time", "Sector2Time", "Sector3Time"]
keep_cols = [
    "Driver", "LapNumber",
    "LapTime", "Sector1Time", "Sector2Time", "Sector3Time",
    "Stint", "Compound", "TyreLife",
    "Team", "TrackStatus"
]

keep = [c for c in keep_cols if c in laps.columns]
b_laps = laps[keep].copy()

## convert to seconds (floats)
for c in [c for c in time_cols if c in b_laps.columns]:
    b_laps[c] = pd.to_timedelta(b_laps[c], errors="coerce").dt.total_seconds()

## add identifiers
b_laps["SessionName"] = session_name
b_laps["EventName"]   = str(event_name)
b_laps["Round"]       = round_no
b_laps["Season"]      = SEASON

table_bronze_laps = f"{CAT}.{SCH}.bronze_laps"

## upsert-by-session: delete existing rows for this session, then append
if spark.catalog.tableExists(table_bronze_laps):
    spark.sql(f"""
        DELETE FROM {table_bronze_laps}
        WHERE Season = {SEASON}
          AND Round = {round_no}
          AND SessionName = '{session_name}'
    """)
    mode = "append"
else:
    mode = "overwrite"  ## first ever run creates the table

spark.createDataFrame(b_laps).write.mode(mode).saveAsTable(table_bronze_laps)
print("bronze_laps upserted for:", SEASON, round_no, session_name)


# telemetry (best lap per stint)


trows = []
if "Stint" in laps.columns:
    for (drv, stint), dlap in laps.groupby(["Driver", "Stint"]):
        if pd.isna(stint):
            continue
        try:
            best = dlap.pick_fastest()   ## best lap in that stint
            tel  = best.get_car_data().add_distance()

            keep_t = [c for c in ["Time","Distance","Speed",
                                  "Throttle","Brake","nGear"]
                      if c in tel.columns]
            sub = tel[keep_t].copy()

            sub["Driver"]      = drv
            lap_no = int(best.get("LapNumber", best["LapNumber"]))
            sub["LapNumber"]   = lap_no
            sub["Stint"]       = int(stint)

            try:
                comp = best.get("Compound", None)
            except Exception:
                comp = None
            if comp is None and "Compound" in getattr(best, "index", []):
                comp = best["Compound"]
            sub["Compound"]    = str(comp) if comp is not None else None

            sub["SessionName"] = session_name
            sub["EventName"]   = str(event_name)
            sub["Round"]       = round_no
            sub["Season"]      = SEASON

            trows.append(sub)
        except Exception as e:
            print(f"Telemetry ingest failed for {drv}, stint={stint}: {e}")
            continue

table_bronze_telem = f"{CAT}.{SCH}.bronze_telemetry"

if trows:
    bronze_telem_df = pd.concat(trows, ignore_index=True)

    if spark.catalog.tableExists(table_bronze_telem):
        spark.sql(f"""
            DELETE FROM {table_bronze_telem}
            WHERE Season = {SEASON}
              AND Round = {round_no}
              AND SessionName = '{session_name}'
        """)
        mode_telem = "append"
    else:
        mode_telem = "overwrite"

    spark.createDataFrame(bronze_telem_df).write.mode(mode_telem).saveAsTable(table_bronze_telem)
    print("bronze_telemetry upserted for:", SEASON, round_no, session_name)
else:
    print("WARNING: no telemetry rows ingested for this session.")

# sessions metadata (historical)


table_bronze_sessions = f"{CAT}.{SCH}.bronze_sessions"

if spark.catalog.tableExists(table_bronze_sessions):
    # remove existing row for this session & insert fresh metadata
    spark.sql(f"""
        DELETE FROM {table_bronze_sessions}
        WHERE season = {SEASON}
          AND round = {round_no}
          AND session_name = '{session_name}'
    """)

    spark.sql(f"""
        INSERT INTO {table_bronze_sessions}
        SELECT DISTINCT
          Season      AS season,
          EventName   AS event,
          Round       AS round,
          SessionName AS session_name,
          CAST(NULL AS STRING) AS session_start,
          CAST(NULL AS STRING) AS session_end
        FROM {table_bronze_laps}
        WHERE Season = {SEASON}
          AND Round = {round_no}
          AND SessionName = '{session_name}'
    """)
else:
    # first time: create the table with this one session
    spark.sql(f"""
        CREATE TABLE {table_bronze_sessions} AS
        SELECT DISTINCT
          Season      AS season,
          EventName   AS event,
          Round       AS round,
          SessionName AS session_name,
          CAST(NULL AS STRING) AS session_start,
          CAST(NULL AS STRING) AS session_end
        FROM {table_bronze_laps}
    """)

print("bronze_sessions upserted for:", SEASON, round_no, session_name)
print("Bronze ingest complete:", SEASON, EVENT, SESSION)
