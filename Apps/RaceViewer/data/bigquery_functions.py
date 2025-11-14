import pandas as pd
from google.cloud import bigquery
import streamlit as st

PROJECT_ID = "horseracing-pacey32-github"
DATASET = "horseracescrape"


# ---------------------------
# Private BQ Client
# ---------------------------
def _get_bq_client():
    import os
    import json
    from google.oauth2 import service_account
    from google.cloud import bigquery

    # Load JSON string from Render environment variable
    json_str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if json_str is None:
        raise ValueError("Environment variable GOOGLE_APPLICATION_CREDENTIALS_JSON is missing.")

    # Convert JSON string → dict
    info = json.loads(json_str)

    # Create credentials object
    creds = service_account.Credentials.from_service_account_info(info)

    # Return authenticated BigQuery client
    return bigquery.Client(credentials=creds, project=info["project_id"])


# ---------------------------
# 1. Get list of races for a specific date
# ---------------------------
@st.cache_data(show_spinner="Loading races for selected date...")
def get_races_for_date(date_value):
    client = _get_bq_client()

    query = f"""
    SELECT DISTINCT
        PARSE_DATE('%d/%m/%Y', Pre_RaceDate) AS Pre_RaceDate,
        Pre_RaceLocation,
        Pre_RaceTime,
        Pre_SourceURL
    FROM `{PROJECT_ID}.{DATASET}.RaceFull_Latest`
    WHERE PARSE_DATE('%d/%m/%Y', Pre_RaceDate) = @dt
    ORDER BY Pre_RaceLocation, Pre_RaceTime
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dt", "DATE", date_value)
            ]
        )
    )

    df = job.result().to_dataframe()

    # We already produced proper DATE() via PARSE_DATE
    # but enforce to python date for consistency:
    if "Pre_RaceDate" in df.columns:
        df["Pre_RaceDate"] = pd.to_datetime(df["Pre_RaceDate"]).dt.date

    return df


# ---------------------------
# 2. Load full race details for a single race
# ---------------------------
@st.cache_data(show_spinner="Loading race data...")
def get_single_race(pre_source_url):
    client = _get_bq_client()

    query = f"""
    WITH spine_latest AS (
        SELECT
            prerace_URL,
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
        ON f.Pre_SourceURL = s.prerace_URL
       AND s.rn = 1
    WHERE f.Pre_SourceURL = @url
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("url", "STRING", pre_source_url)
            ]
        )
    )

    df = job.result().to_dataframe()

    # Convert date properly
    if "Pre_RaceDate" in df.columns:
        df["Pre_RaceDate"] = pd.to_datetime(
            df["Pre_RaceDate"], dayfirst=True, errors="coerce"
        ).dt.date

    return df


# ---------------------------
# 3. Totals aggregation (used in Totals tab)
# ---------------------------
def get_totals(df, entity):
    df = df.copy()

    # win/place logic
    def is_win(pos):
        if pd.isna(pos): return False
        s = str(pos).lower()
        for suf in ["st", "nd", "rd", "th"]:
            if s.endswith(suf):
                s = s[:-len(suf)]
        return s.isdigit() and int(s) == 1

    def is_place(pos):
        if pd.isna(pos): return False
        s = str(pos).lower()
        for suf in ["st", "nd", "rd", "th"]:
            if s.endswith(suf):
                s = s[:-len(suf)]
        return s.isdigit() and int(s) <= 3

    df["Win"] = df["Pos"].apply(is_win)
    df["Place"] = df["Pos"].apply(is_place)

    if "PrizeMoney" not in df.columns:
        df["PrizeMoney"] = 0

    # Entity
    if entity == "Horses":
        df["Entity"] = df["Post_HorseName"].fillna(df["HorseName"])
    elif entity == "Jockeys":
        df["Entity"] = df["Post_Jockey"].fillna(df["Jockey"])
    else:
        df["Entity"] = df["Post_Trainer"].fillna(df["Trainer"])

    df = df.dropna(subset=["Entity"])

    summary = (
        df.groupby("Entity")
        .agg(
            Races=("Pre_SourceURL", "nunique"),
            Wins=("Win", "sum"),
            Places=("Place", "sum"),
            TotalPrizeMoney=("PrizeMoney", "sum"),
        )
        .reset_index()
    )

    summary["WinPct"] = (summary["Wins"] / summary["Races"]).round(3)
    summary = summary.sort_values("TotalPrizeMoney", ascending=False)

    return summary