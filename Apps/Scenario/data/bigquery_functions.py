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
    json_str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if json_str is None:
        raise ValueError("Environment variable GOOGLE_APPLICATION_CREDENTIALS_JSON is missing.")

    info = json.loads(json_str)
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=creds, project=info["project_id"])


@st.cache_data(show_spinner="Loading scenario dataset...", ttl=3600)
def get_scenario_base(date_from, date_to):
    """
    Returns runner-level rows across the selected date range.

    Expected to include:
      RaceDateTime (TIMESTAMP or DATETIME), RaceLocation, HorseName, Odds_dec,
      Result_Position, Post_RaceRunners, HandicappedRace, RaceHistoryStats,
      plus anything else you want to filter on.
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
    Favourite
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

    # Normalise types
    if "RaceDateTime" in df.columns:
        df["RaceDateTime"] = pd.to_datetime(df["RaceDateTime"], errors="coerce")

    return df