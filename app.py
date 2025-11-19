import os
import pandas as pd
import streamlit as st
from databricks import sql

# streamlit page config

st.set_page_config(
    page_title="Cadillac F1 – Race Stint & Reliability Explorer",
    layout="wide"
)

st.title("Cadillac F1 – Race Stint & Reliability Explorer")
st.write(
    "Explore stint performance, telemetry reliability, and sector pace"
    "created with databricks and fastf1"
)


# databricks connection configuration


DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME", "dbc-d5cff1fc-6289.cloud.databricks.com")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/ddcd27f4944c66d7")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "########################################")

CAT = "workspace"
SCH = "f1"


# helper functions


def get_connection():
    """Create a Databricks SQL connection."""
    return sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )


@st.cache_data(show_spinner=True)
def load_sessions():
    """
    Load available sessions from the lakehouse.

    This assumes that your gold table `gold_stints_with_reliability`
    contains one row per (Season, Round, SessionName, Driver, Stint, Compound).

    If in the future you create a sessions_history table, you can swap
    this query to use that instead.
    """
    query = f"""
        SELECT DISTINCT
            Season,
            Round,
            SessionName
        FROM {CAT}.{SCH}.gold_stints_with_reliability
        ORDER BY Season DESC, Round DESC, SessionName
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    return df


@st.cache_data(show_spinner=True)
def load_stints(season: int, round_: int, session_name: str) -> pd.DataFrame:
    query = f"""
        SELECT
          Driver,
          Stint,
          Compound,
          Laps,
          AvgLap,
          BestLap,
          RelScoreAvg,
          Reliability,
          DropoutPctFmt,
          SpeedDataRatioFmt
        FROM {CAT}.{SCH}.v_gold_stints_with_reliability_fmt
        WHERE Season = {season}
          AND Round = {round_}
          AND SessionName = '{session_name}'
        ORDER BY Driver, Stint
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    return df


@st.cache_data(show_spinner=True)
def load_stint_deltas(season: int, round_: int, session_name: str) -> pd.DataFrame:
    query = f"""
        SELECT
          Driver,
          Stint,
          Compound,
          Laps,
          AvgLap,
          BestLap,
          DeltaBestToClassLeader,
          DeltaBestToClassLeader_s
        FROM {CAT}.{SCH}.v_gold_stints_deltas_fmt
        WHERE Season = {season}
          AND Round = {round_}
          AND SessionName = '{session_name}'
        ORDER BY Driver, Stint
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    return df


@st.cache_data(show_spinner=True)
def load_sectors(season: int, round_: int, session_name: str) -> pd.DataFrame:
    query = f"""
        SELECT
          Driver,
          AvgLap,
          S1,
          S2,
          S3,
          AvgLap_s,
          S1_s,
          S2_s,
          S3_s
        FROM {CAT}.{SCH}.v_gold_sectors_fmt
        WHERE Season = {season}
          AND Round = {round_}
          AND SessionName = '{session_name}'
        ORDER BY Driver
    """
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    return df



# connection sanity check


if any(x.startswith("YOUR_") for x in [
    DATABRICKS_SERVER_HOSTNAME,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_TOKEN,
]):
    st.error(
        " Databricks connection is not configured.\n\n"
    )
    st.stop()


# sidebar: session pick


with st.sidebar:
    st.header("📅 Session selector")

    try:
        sessions_df = load_sessions()
    except Exception as e:
        st.error(f"Error loading sessions list: {e}")
        st.stop()

    if sessions_df.empty:
        st.warning(
            "No sessions found in `gold_stints_with_reliability`.\n\n"
            "Make sure you've run your ingestion + transform notebooks "
            "and that they *append* to the gold tables for each new race."
        )
        st.stop()

    sessions_df["label"] = sessions_df.apply(
        lambda r: f"{int(r['Season'])} – R{int(r['Round'])} – {r['SessionName']}",
        axis=1
    )

    selected_label = st.selectbox(
        "Choose a race session:",
        options=sessions_df["label"].tolist(),
    )

    selected_row = sessions_df.loc[sessions_df["label"] == selected_label].iloc[0]
    selected_season = int(selected_row["Season"])
    selected_round = int(selected_row["Round"])
    selected_session = str(selected_row["SessionName"])

    st.markdown(
        f"**Selected:** Season **{selected_season}**, Round **{selected_round}**, "
        f"Session **{selected_session}**"
    )

    st.markdown("---")
    st.caption(
        "Tip: In your Databricks transform notebook, use **append mode** "
        "for your gold tables to build up historical races."
    )


#  tabs


tab1, tab2, tab3 = st.tabs([
    "🏁 Stints & Reliability",
    "📊 Stint Deltas vs Compound Leader",
    "⏱️ Sector Pace",
])

# stints

with tab1:
    st.subheader("Stints & Telemetry Reliability")

    try:
        stints_df = load_stints(selected_season, selected_round, selected_session)
    except Exception as e:
        st.error(f"Error loading stints: {e}")
        st.stop()

    if stints_df.empty:
        st.info("No stint data found for this session.")
    else:
        drivers = ["(All)"] + sorted(stints_df["Driver"].unique().tolist())
        selected_driver = st.selectbox("Filter by driver", drivers, index=0)

        df_view = stints_df.copy()
        if selected_driver != "(All)":
            df_view = df_view[df_view["Driver"] == selected_driver]

        st.dataframe(df_view, use_container_width=True)

        st.markdown("#### Average Reliability Score by Driver")
        rel_plot = (
            stints_df.groupby("Driver", as_index=False)["RelScoreAvg"]
            .mean()
            .sort_values("RelScoreAvg", ascending=False)
        )
        rel_plot = rel_plot.set_index("Driver")
        st.bar_chart(rel_plot, use_container_width=True)


# stint deltas

with tab2:
    st.subheader("Stint Delta vs Best in Compound")

    try:
        deltas_df = load_stint_deltas(selected_season, selected_round, selected_session)
    except Exception as e:
        st.error(f"Error loading stint deltas: {e}")
        deltas_df = pd.DataFrame()

    if deltas_df.empty:
        st.info("No stint delta data available for this session.")
    else:
        st.write(
            "Each row shows a driver's best lap in a stint and the delta to the best lap "
            "on the same compound in this session."
        )

        st.dataframe(deltas_df, use_container_width=True)

        st.markdown("#### Delta to Compound Leader (seconds)")
        delta_plot = (
            deltas_df[["Driver", "Stint", "DeltaBestToClassLeader_s"]]
            .copy()
        )
        delta_plot["label"] = delta_plot["Driver"] + " S" + delta_plot["Stint"].astype(str)
        delta_plot = delta_plot.set_index("label")
        st.bar_chart(delta_plot["DeltaBestToClassLeader_s"], use_container_width=True)


# sectors

with tab3:
    st.subheader("Average Sector Pace")

    try:
        sectors_df = load_sectors(selected_season, selected_round, selected_session)
    except Exception as e:
        st.error(f"Error loading sectors: {e}")
        sectors_df = pd.DataFrame()

    if sectors_df.empty:
        st.info("No sector data available for this session.")
    else:
        st.write("All times are averages per driver over the selected session.")

        st.dataframe(sectors_df, use_container_width=True)


        st.markdown("#### Sector Times (seconds) by Driver")
        sectors_plot = sectors_df.set_index("Driver")[["S1_s", "S2_s", "S3_s"]]
        st.bar_chart(sectors_plot, use_container_width=True)

st.success("App ready. Select different sessions from the sidebar to explore your stored race data.")
