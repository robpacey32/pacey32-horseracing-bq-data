import os
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st


PROJECT_ID = "horseracing-pacey32-github"
DATASET = "horseracescrape"


def _get_bq_client() -> bigquery.Client:
    """
    Use GOOGLE_APPLICATION_CREDENTIALS_JSON if set (Render),
    otherwise default credentials (local dev).
    """
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=credentials, project=info["project_id"])
    else:
        return bigquery.Client(project=PROJECT_ID)


@st.cache_data(show_spinner="Loading race data from BigQuery...")
def load_race_data():
    client = _get_bq_client()

    query = f"""
    WITH spine_latest AS (
        SELECT
            prerace_URL,
            postrace_URL,
            Status,
            load_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY prerace_URL
                ORDER BY load_timestamp DESC
            ) AS rn
        FROM `{PROJECT_ID}.{DATASET}.RaceSpine_Latest`
    )

    SELECT
        f.*,
        s.Status AS RaceStatus
    FROM `{PROJECT_ID}.{DATASET}.RaceFull_Latest` f
    LEFT JOIN spine_latest s
        ON f.Pre_SourceURL = s.prerace_URL    -- 🔥 FIXED JOIN
       AND s.rn = 1
    """

    df = client.query(query).result().to_dataframe()

    # Convert Pre_RaceDate to actual dates
    if "Pre_RaceDate" in df.columns:
        df["Pre_RaceDate"] = pd.to_datetime(df["Pre_RaceDate"]).dt.date

    return df


def get_filter_options(df: pd.DataFrame):
    """Return sorted unique options for date, location & race name."""
    dates = (
        sorted(df["RaceDate"].dropna().unique())
        if "RaceDate" in df.columns else []
    )
    locations = (
        sorted(df["RaceLocation"].dropna().unique())
        if "RaceLocation" in df.columns else []
    )
    races = (
        sorted(df["RaceName_Pre"].dropna().unique())
        if "RaceName_Pre" in df.columns else []
    )
    return dates, locations, races


def filter_by_selection(df: pd.DataFrame, date, location, race_name) -> pd.DataFrame:
    """Filter the master DF to the selected race."""
    out = df.copy()
    if date is not None and "RaceDate" in out.columns:
        out = out[out["RaceDate"] == date]
    if location and location != "—" and "RaceLocation" in out.columns:
        out = out[out["RaceLocation"] == location]
    if race_name and race_name != "—" and "RaceName_Pre" in out.columns:
        out = out[out["RaceName_Pre"] == race_name]
    return out
