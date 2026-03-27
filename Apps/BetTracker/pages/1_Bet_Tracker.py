# -------------------------
# IMPORTS
# -------------------------
from pathlib import Path
import sys

import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, date
from google.cloud import bigquery
from google.oauth2 import service_account

# -------------------------
# PAGE CONFIG
# -------------------------
st.set_page_config(
    page_title="Bet Tracker",
    page_icon="🐎",
    layout="wide"
)

# -------------------------
# REPO ROOT / IMPORT PATH
# -------------------------
CURRENT_FILE = Path(__file__).resolve()
for parent in CURRENT_FILE.parents:
    if (parent / "shared").exists():
        if str(parent) not in sys.path:
            sys.path.insert(0, str(parent))
        break

from shared.styles import load_app_css
from shared.ui_auth import (
    configure_ui_auth,
    render_login_portal,
    get_current_user,
    logout,
)
from shared.config import get_config

load_app_css()

#with st.sidebar:
#    st.markdown(
#        "<h2 style='margin-bottom:0.5rem;'>Bet Tracker</h2>",
#        unsafe_allow_html=True
#    )

# -------------------------
# CSS / THEME
# -------------------------
def apply_bettracker_theme():
    css_path = Path(__file__).with_name("styles.css")
    if css_path.exists():
        with open(css_path, "r") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# -------------------------
# SHARED AUTH CONFIG FOR THIS APP
# -------------------------
configure_ui_auth(
    session_days=30,
    session_storage_key="bettracker_session_token",
    help_email="info@pacey32.com",
)

# -------------------------
# ACCOUNT SIGN IN
# -------------------------
user = get_current_user()

if not user:
    render_login_portal(show_title=True)
    st.stop()

with st.sidebar:
    st.write(f"Signed in as: {user.get('email', 'Unknown user')}")
    if st.button("Log out"):
        logout()
        st.rerun()

# -------------------------
# CURRENT USER
# -------------------------
CURRENT_USER_ID = str(user["user_id"])
CURRENT_USER_EMAIL = user.get("email")
CURRENT_USER_NAME = user.get("username")

# -------------------------
# CONFIG
# -------------------------
cfg = get_config()

MONGO_URI = cfg["MONGO_URI"]
MONGO_DB_NAME = cfg.get("APP_MONGO_DB_NAME", "bettingapp")
MONGO_COLLECTION_NAME = cfg.get("APP_MONGO_COLLECTION_NAME", "bet_selections")

KEY_PATH = cfg["KEY_PATH"]
PROJECT_ID = cfg["PROJECT_ID"]
VIEW_ID = cfg["VIEW_ID"]
BQ_SELECTION_TABLE = cfg.get(
    "BQ_SELECTION_TABLE",
    "horseracing-pacey32-github.bettingapp.bet_selections",
)

# -------------------------
# CONNECTIONS
# -------------------------
@st.cache_resource
def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    collection.create_index([("user_id", 1), ("race_date", 1)])
    collection.create_index([("user_id", 1), ("runner_id", 1)], unique=True)

    return collection


@st.cache_resource
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


bet_collection = get_mongo_collection()
bq_client = get_bigquery_client()

# -------------------------
# DATA LOADERS
# -------------------------
@st.cache_data(ttl=60)
def load_runners(selected_date):
    query = f"""
        SELECT *
        FROM `{VIEW_ID}`
        WHERE RaceDate = @selected_date
        ORDER BY RaceTime, RaceLocation, RaceName, HorseName
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("selected_date", "DATE", selected_date)
        ]
    )

    return bq_client.query(query, job_config=job_config).to_dataframe(
        create_bqstorage_client=False
    )


@st.cache_data(ttl=10)
def load_existing_selections_mongo(user_id):
    docs = list(
        bet_collection.find(
            {"user_id": user_id},
            {"_id": 0, "runner_id": 1}
        )
    )
    return {str(doc["runner_id"]) for doc in docs if "runner_id" in doc}


@st.cache_data(ttl=10)
def load_my_selections_from_mongo(selected_date, user_id):
    docs = list(
        bet_collection.find(
            {
                "user_id": user_id,
                "race_date": str(selected_date)
            },
            {"_id": 0}
        )
    )
    return pd.DataFrame(docs)

# -------------------------
# ACTIONS
# -------------------------
def insert_selection(row, user_id, user_email, user_name, stake=10.0):
    runner_id = str(row["runner_id"])

    existing = bet_collection.find_one({
        "user_id": user_id,
        "runner_id": runner_id
    })

    if existing:
        return False, "Already selected"

    race_date_value = row["RaceDate"].date() if hasattr(row["RaceDate"], "date") else row["RaceDate"]
    odds_value = row["Odds"] if "Odds" in row and pd.notna(row["Odds"]) else None
    form_value = row["Form"] if "Form" in row and pd.notna(row["Form"]) else None
    created_at_utc = datetime.utcnow()

    bq_row = {
        "user_id": user_id,
        "user_email": user_email,
        "user_name": user_name,
        "runner_id": runner_id,
        "race_id": str(row["race_id"]),
        "race_date": race_date_value.isoformat() if race_date_value else None,
        "race_time": str(row["RaceTime"]),
        "race_location": str(row["RaceLocation"]),
        "race_name": str(row["RaceName"]),
        "horse_name": str(row["HorseName"]),
        "form": str(form_value) if form_value is not None else None,
        "odds": str(odds_value) if odds_value is not None else None,
        "stake": float(stake),
        "created_at": created_at_utc.isoformat()
    }

    errors = bq_client.insert_rows_json(BQ_SELECTION_TABLE, [bq_row])

    if errors:
        return False, f"BigQuery insert failed: {errors}"

    mongo_doc = {
        "user_id": user_id,
        "user_email": user_email,
        "user_name": user_name,
        "runner_id": runner_id,
        "race_id": str(row["race_id"]),
        "race_date": str(race_date_value),
        "race_time": str(row["RaceTime"]),
        "race_location": str(row["RaceLocation"]),
        "race_name": str(row["RaceName"]),
        "horse_name": str(row["HorseName"]),
        "form": str(form_value) if form_value is not None else None,
        "odds": str(odds_value) if odds_value is not None else None,
        "stake": float(stake),
        "created_at": created_at_utc
    }

    bet_collection.insert_one(mongo_doc)

    return True, "Saved"


def remove_selection(runner_id, user_id):
    runner_id = str(runner_id)

    bet_collection.delete_one({
        "user_id": user_id,
        "runner_id": runner_id
    })

    query = f"""
        DELETE FROM `{BQ_SELECTION_TABLE}`
        WHERE user_id = @user_id
          AND runner_id = @runner_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("runner_id", "STRING", runner_id),
        ]
    )

    bq_client.query(query, job_config=job_config).result()
    return True

# -------------------------
# APP
# -------------------------
st.title("Horse Racing Betting Tracker")

user_id = CURRENT_USER_ID

# -------------------------
# TOP CONTROLS
# -------------------------
top_col1, top_col2, top_col3 = st.columns([2, 1, 1])

with top_col1:
    selected_date = st.date_input("Race Date", value=date.today())

df = load_runners(selected_date)
selected_runner_ids = load_existing_selections_mongo(user_id=user_id)
my_selections_df = load_my_selections_from_mongo(selected_date, user_id=user_id)

if df.empty:
    st.warning("No runners found for the selected date.")
    st.stop()

with top_col2:
    time_options_top = ["All"] + sorted(df["RaceTime"].dropna().astype(str).unique().tolist())
    selected_time = st.selectbox("Race Time", time_options_top)

with top_col3:
    default_stake = st.number_input("Default Stake", min_value=0.0, value=10.0, step=1.0)

# -------------------------
# LOCATION
# -------------------------
location_options = ["All"] + sorted(df["RaceLocation"].dropna().astype(str).unique().tolist())
selected_location = st.radio("Location", location_options, horizontal=True)

# -------------------------
# SEARCH
# -------------------------
horse_search = st.text_input("Search horse name")

# -------------------------
# FILTERING
# -------------------------
filtered_df = df.copy()

if selected_location != "All":
    filtered_df = filtered_df[
        filtered_df["RaceLocation"].astype(str) == selected_location
    ]

if selected_time != "All":
    filtered_df = filtered_df[
        filtered_df["RaceTime"].astype(str) == selected_time
    ]

if horse_search.strip():
    filtered_df = filtered_df[
        filtered_df["HorseName"].astype(str).str.contains(horse_search.strip(), case=False, na=False)
    ]

# -------------------------
# DIVIDER
# -------------------------
st.markdown(
    "<hr style='border: 1px solid #8FA58C;'>",
    unsafe_allow_html=True
)

# -------------------------
# SELECTED HORSES
# -------------------------
st.markdown("### Selected Horses")

if my_selections_df.empty:
    st.write("No selections yet.")
else:
    display_cols = [
        c for c in [
            "race_time",
            "race_location",
            "race_name",
            "horse_name",
            "form",
            "odds",
            "stake"
        ] if c in my_selections_df.columns
    ]

    st.dataframe(
        my_selections_df[display_cols],
        width="stretch",
        hide_index=True
    )

# -------------------------
# DIVIDER
# -------------------------
st.markdown(
    "<hr style='border: 1px solid #8FA58C;'>",
    unsafe_allow_html=True
)

# -------------------------
# RUNNERS
# -------------------------
st.markdown("### Runners")

if filtered_df.empty:
    st.warning("No runners match the current filters.")
else:
    races = filtered_df.groupby(["RaceLocation", "RaceTime", "RaceName"], sort=False)

    for (location, time_, race_name), race_df in races:
        with st.expander(f"{location} {time_} — {race_name}", expanded=False):
            for _, row in race_df.iterrows():
                runner_id = str(row["runner_id"])
                already_selected = runner_id in selected_runner_ids

                col1, col2, col3, col4, col5 = st.columns([4, 2, 2, 2, 1])

                with col1:
                    horse_name = str(row["HorseName"])
                    st.write(f"✅ {horse_name}" if already_selected else horse_name)

                with col2:
                    form_value = row["Form"] if "Form" in row and pd.notna(row["Form"]) else ""
                    st.write(form_value)

                with col3:
                    odds_value = row["Odds"] if "Odds" in row and pd.notna(row["Odds"]) else ""
                    st.write(odds_value)

                with col4:
                    st.write("Selected" if already_selected else "")

                with col5:
                    if already_selected:
                        if st.button("Unselect", key=f"unselect_{runner_id}"):
                            remove_selection(runner_id, user_id=user_id)
                            st.cache_data.clear()
                            st.rerun()
                    else:
                        if st.button("Select", key=f"select_{runner_id}"):
                            ok, message = insert_selection(
                                row=row,
                                user_id=user_id,
                                user_email=CURRENT_USER_EMAIL,
                                user_name=CURRENT_USER_NAME,
                                stake=default_stake
                            )
                            if ok:
                                st.success(f"Saved selection: {row['HorseName']}")
                            else:
                                st.warning(message)
                            st.cache_data.clear()
                            st.rerun()
