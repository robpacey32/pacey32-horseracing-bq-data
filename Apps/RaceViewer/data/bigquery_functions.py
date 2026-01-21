import os
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st

PROJECT_ID = "horseracing-pacey32-github"
DATASET = "horseracescrape"
ANALYTICS_DATASET = "horseraceanalytics"
RACE_TOTALS_VIEW = f"{PROJECT_ID}.{ANALYTICS_DATASET}.RaceTotalsForApp"


# ---------------------------
# Private BQ Client
# ---------------------------
def _get_bq_client():
    # Load JSON string from Render environment variable
    json_str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if json_str is None:
        raise ValueError("Environment variable GOOGLE_APPLICATION_CREDENTIALS_JSON is missing.")

    info = json.loads(json_str)
    creds = service_account.Credentials.from_service_account_info(info)
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
            query_parameters=[bigquery.ScalarQueryParameter("dt", "DATE", date_value)]
        ),
    )

    df = job.result().to_dataframe()
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
            query_parameters=[bigquery.ScalarQueryParameter("url", "STRING", pre_source_url)]
        ),
    )

    df = job.result().to_dataframe()

    if "Pre_RaceDate" in df.columns:
        df["Pre_RaceDate"] = pd.to_datetime(df["Pre_RaceDate"], dayfirst=True, errors="coerce").dt.date

    return df


# ---------------------------
# 3. Totals aggregation (used in Totals expander)
# ---------------------------
def get_totals(df, entity):
    df = df.copy()

    def _pos_to_int(pos):
        if pd.isna(pos):
            return None
        s = str(pos).lower().strip()
        for suf in ["st", "nd", "rd", "th"]:
            if s.endswith(suf):
                s = s[:-len(suf)]
        return int(s) if s.isdigit() else None

    df["PosInt"] = df["Pos"].apply(_pos_to_int)
    df["Win"] = df["PosInt"].eq(1)
    df["Place"] = df["PosInt"].le(3)  # (app totals only; your correct EW logic is in the BQ view)

    if "PrizeMoney" not in df.columns:
        df["PrizeMoney"] = 0

    if entity == "Horses":
        df["Entity"] = df.get("Post_HorseName", pd.Series([None] * len(df))).fillna(df.get("HorseName"))
    elif entity == "Jockeys":
        df["Entity"] = df.get("Post_Jockey", pd.Series([None] * len(df))).fillna(df.get("Jockey"))
    else:
        df["Entity"] = df.get("Post_Trainer", pd.Series([None] * len(df))).fillna(df.get("Trainer"))

    df = df.dropna(subset=["Entity"])

    summary = (
        df.groupby("Entity", dropna=True)
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


# ---------------------------
# 4. Last 12 months stats (for selected race entities) via RaceTotalsForApp view
# ---------------------------
@st.cache_data(show_spinner="Loading last 12 months stats...", ttl=3600)
def get_last12m_entity_stats(names, as_of_date, entity_type):
    """
    names: list[str] - entities in the selected race
    as_of_date: datetime.date (selected race date)
    entity_type: 'HORSE' | 'JOCKEY' | 'TRAINER'
    """
    if not names:
        return pd.DataFrame(columns=["Entity", "Races", "Wins", "Places", "PrizeMoneyTotal"])

    client = _get_bq_client()

    query = f"""
    SELECT
      Entity,
      COUNT(*) AS Races,
      SUM(CASE WHEN PosInt = 1 THEN 1 ELSE 0 END) AS Wins,
      SUM(COALESCE(Placed, 0)) AS Places,
      SUM(COALESCE(PrizeMoneyNumeric, 0)) AS PrizeMoneyTotal
    FROM (
      SELECT
        CASE
          WHEN @entity_type = 'HORSE' THEN HorseName
          WHEN @entity_type = 'JOCKEY' THEN Jockey
          WHEN @entity_type = 'TRAINER' THEN Trainer
          ELSE NULL
        END AS Entity,
        PosInt,
        Placed,
        PrizeMoneyNumeric
      FROM `{RACE_TOTALS_VIEW}`
      WHERE RaceDate BETWEEN DATE_SUB(@as_of_date, INTERVAL 12 MONTH) AND @as_of_date
    )
    WHERE Entity IN UNNEST(@names)
      AND Entity IS NOT NULL
    GROUP BY Entity
    ORDER BY PrizeMoneyTotal DESC, Wins DESC, Places DESC, Races DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("as_of_date", "DATE", as_of_date),
            bigquery.ScalarQueryParameter("entity_type", "STRING", entity_type),
            bigquery.ArrayQueryParameter("names", "STRING", names),
        ]
    )

    return client.query(query, job_config=job_config).result().to_dataframe()