import os
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st

PROJECT_ID = "horseracing-pacey32-github"
ANALYTICS_DATASET = "horseraceanalytics"

SCENARIO_BASE_VIEW = "horseracing-pacey32-github.horseraceanalytics.Scenario_1_DataPrep_vw"


def _get_bq_client():
    # Option 1: full JSON stored in env var
    json_str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if json_str:
        try:
            info = json.loads(json_str)
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(
                project=info.get("project_id", PROJECT_ID),
                credentials=credentials
            )
        except json.JSONDecodeError:
            raise ValueError(
                "GOOGLE_APPLICATION_CREDENTIALS_JSON is set, but it does not contain valid JSON. "
                "If you are supplying a file path, use GOOGLE_APPLICATION_CREDENTIALS instead."
            )

    # Option 2: local file path stored in env var
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if key_path:
        if not os.path.exists(key_path):
            raise ValueError(
                f"GOOGLE_APPLICATION_CREDENTIALS points to a file that does not exist: {key_path}"
            )

        credentials = service_account.Credentials.from_service_account_file(key_path)
        return bigquery.Client(
            project=credentials.project_id or PROJECT_ID,
            credentials=credentials
        )

    # Option 3: Streamlit secrets
    if "GOOGLE_APPLICATION_CREDENTIALS_JSON" in st.secrets:
        try:
            info = json.loads(st.secrets["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(
                project=info.get("project_id", PROJECT_ID),
                credentials=credentials
            )
        except json.JSONDecodeError:
            raise ValueError(
                "GOOGLE_APPLICATION_CREDENTIALS_JSON in st.secrets is not valid JSON."
            )

    raise ValueError(
        "No BigQuery credentials found. "
        "Set GOOGLE_APPLICATION_CREDENTIALS to your local key file path, "
        "or GOOGLE_APPLICATION_CREDENTIALS_JSON to the full JSON content."
    )


@st.cache_data(show_spinner="Loading scenario dataset...", ttl=0)
def get_scenario_base(date_from, date_to):
    """
    Returns runner-level rows across the selected date range.

    Includes:
      Existing scenario fields used by the app, plus:
      LastRun_num, Age_num, Weight_lbs, Distance_Furlongs,
      Going_Standard, Going_Soft, Going_Good, Going_Heavy, Going_GtF, Going_GtS,
      Track_Turf, Track_AW, Track_Poly
    """
    client = _get_bq_client()

    query = f"""
    SELECT
        DATE(RaceDateTime) AS RaceDate,
        RaceDateTime,
        RaceDateDt,
        Pre_RaceLocation AS RaceLocation,
        Pre_RaceTime AS RaceTime,
        HorseName,
        Odds,
        Odds_dec,
        Result_Position,
        Post_RaceRunners,
        HandicappedRace,
        RaceHistoryStats,
        Favourite,
        Form,
        WonInLast3,
        Win2InLast6,

        -- new numeric fields
        SAFE_CAST(REGEXP_EXTRACT(CAST(LastRun AS STRING), r'(\\d+)') AS INT64) AS LastRun_num,
        SAFE_CAST(REGEXP_EXTRACT(CAST(Age AS STRING), r'(\\d+)') AS INT64) AS Age_num,
        (
            COALESCE(SAFE_CAST(REGEXP_EXTRACT(CAST(Weight AS STRING), r'^(\\d+)') AS INT64), 0) * 14
            + COALESCE(SAFE_CAST(REGEXP_EXTRACT(CAST(Weight AS STRING), r'-(\\d+)$') AS INT64), 0)
        ) AS Weight_lbs,

        (
            COALESCE(SAFE_CAST(REGEXP_EXTRACT(CAST(Pre_RaceDistance AS STRING), r'(\\d+)m') AS FLOAT64), 0) * 8
            + COALESCE(SAFE_CAST(REGEXP_EXTRACT(CAST(Pre_RaceDistance AS STRING), r'(\\d+)f') AS FLOAT64), 0)
            + COALESCE(SAFE_CAST(REGEXP_EXTRACT(CAST(Pre_RaceDistance AS STRING), r'(\\d+)y') AS FLOAT64), 0) / 220
        ) AS Distance_Furlongs,

        -- going flags
        REGEXP_CONTAINS(LOWER(COALESCE(CAST(Pre_RaceGoing AS STRING), '')), r'standard') AS Going_Standard,
        REGEXP_CONTAINS(LOWER(COALESCE(CAST(Pre_RaceGoing AS STRING), '')), r'soft') AS Going_Soft,
        REGEXP_CONTAINS(LOWER(COALESCE(CAST(Pre_RaceGoing AS STRING), '')), r'good') AS Going_Good,
        REGEXP_CONTAINS(LOWER(COALESCE(CAST(Pre_RaceGoing AS STRING), '')), r'heavy') AS Going_Heavy,
        REGEXP_CONTAINS(LOWER(COALESCE(CAST(Pre_RaceGoing AS STRING), '')), r'good to firm|firm') AS Going_GtF,
        REGEXP_CONTAINS(LOWER(COALESCE(CAST(Pre_RaceGoing AS STRING), '')), r'good to soft') AS Going_GtS,

        -- track flags
        LOWER(COALESCE(CAST(Pre_RaceSurface AS STRING), '')) IN ('turf', 'grass') AS Track_Turf,
        LOWER(COALESCE(CAST(Pre_RaceSurface AS STRING), '')) IN ('all-weather', 'aw', 'tapeta', 'polytrack', 'fibresand') AS Track_AW,
        LOWER(COALESCE(CAST(Pre_RaceSurface AS STRING), '')) = 'polytrack' AS Track_Poly

    FROM `{SCENARIO_BASE_VIEW}`
    WHERE DATE(RaceDateTime) BETWEEN @d1 AND @d2
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("d1", "DATE", date_from),
                bigquery.ScalarQueryParameter("d2", "DATE", date_to),
            ]
        ),
    )
    df = job.result().to_dataframe()

    if "RaceDateTime" in df.columns:
        df["RaceDateTime"] = pd.to_datetime(df["RaceDateTime"], errors="coerce")

    return df