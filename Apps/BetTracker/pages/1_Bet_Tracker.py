# -------------------------
# IMPORTS
# -------------------------
from pathlib import Path
import sys
import re

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

# -------------------------
# CSS / THEME
# -------------------------
def apply_bettracker_theme():
    css_path = Path(__file__).with_name("styles.css")
    if css_path.exists():
        with open(css_path, "r") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

apply_bettracker_theme()

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
# ODDS HELPERS
# -------------------------
def normalise_odds_input(odds_value: str | None) -> str | None:
    if odds_value is None:
        return None

    clean = str(odds_value).strip().lower()

    if not clean:
        return None

    if clean in {"evs", "evens", "even"}:
        return "evens"

    if re.fullmatch(r"\d+\s*/\s*\d+", clean):
        num, den = clean.split("/")
        return f"{int(num.strip())}/{int(den.strip())}"

    return None


def fractional_to_decimal(odds_value: str | None) -> float | None:
    normalised = normalise_odds_input(odds_value)

    if normalised is None:
        return None

    if normalised == "evens":
        return 2.0

    num, den = normalised.split("/")
    return round((float(num) / float(den)) + 1.0, 6)


def validate_taken_odds(odds_value: str | None) -> tuple[bool, str | None, float | None]:
    if odds_value is None or str(odds_value).strip() == "":
        return True, None, None

    normalised = normalise_odds_input(odds_value)

    if normalised is None:
        return False, None, None

    odds_dec = fractional_to_decimal(normalised)
    return True, normalised, odds_dec


def normalise_ew_flag(value) -> str:
    if value is None:
        return "N"

    clean = str(value).strip().upper()
    return "Y" if clean == "Y" else "N"


def ew_flag_to_label(value) -> str:
    return "EW" if normalise_ew_flag(value) == "Y" else "Win"


def ew_label_to_flag(value) -> str:
    clean = str(value).strip().upper()
    return "Y" if clean == "EW" else "N"


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
def insert_selection(
    row,
    user_id,
    user_email,
    user_name,
    stake=10.0,
    taken_odds=None,
    taken_odds_dec=None,
    ew_bet="N"
):
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
        "taken_odds": taken_odds,
        "taken_odds_dec": taken_odds_dec,
        "stake": float(stake),
        "ew_bet": normalise_ew_flag(ew_bet),
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
    if "ew_bet" not in my_selections_df.columns:
        my_selections_df["ew_bet"] = "N"

    my_selections_df["ew_bet"] = my_selections_df["ew_bet"].apply(ew_flag_to_label)

    display_cols = [
        c for c in [
            "race_time",
            "race_location",
            "race_name",
            "horse_name",
            "form",
            "odds",
            "taken_odds",
            "stake",
            "ew_bet",
        ] if c in my_selections_df.columns
    ]

    selected_display = my_selections_df[display_cols].copy()

    rename_map = {
        "race_time": "Race Time",
        "race_location": "Location",
        "race_name": "Race Name",
        "horse_name": "Horse",
        "form": "Form",
        "odds": "Odds",
        "taken_odds": "Taken Odds",
        "stake": "Stake",
        "ew_bet": "Bet Type",
    }
    selected_display = selected_display.rename(columns=rename_map)

    if "Stake" in selected_display.columns:
        selected_display["Stake"] = pd.to_numeric(selected_display["Stake"], errors="coerce").fillna(0.0)

    st.dataframe(
        selected_display,
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

                odds_value = row["Odds"] if "Odds" in row and pd.notna(row["Odds"]) else ""
                form_value = row["Form"] if "Form" in row and pd.notna(row["Form"]) else ""
                horse_name = str(row["HorseName"])

                source_odds_normalised = normalise_odds_input(odds_value)
                taken_odds_placeholder = "Taken odds: e.g. 9/1"
                if source_odds_normalised:
                    taken_odds_placeholder = f"Taken odds: {source_odds_normalised}"

                col1, col2, col3, col4, col5, col6, col7 = st.columns([4.0, 1.4, 1.3, 2.6, 1.7, 1.4, 1.5])

                with col1:
                    st.write(f"✅ {horse_name}" if already_selected else horse_name)

                with col2:
                    st.write(form_value)

                with col3:
                    st.write(odds_value)

                with col4:
                    taken_odds_input = st.text_input(
                        "Taken odds",
                        value="",
                        key=f"taken_odds_{runner_id}",
                        placeholder=taken_odds_placeholder,
                        label_visibility="collapsed"
                    )

                with col5:
                    horse_stake = st.number_input(
                        "Stake",
                        min_value=0.0,
                        value=float(default_stake),
                        step=1.0,
                        key=f"stake_{runner_id}",
                        label_visibility="collapsed"
                    )

                with col6:
                    bet_type = st.selectbox(
                        "Bet Type",
                        options=["Win", "EW"],
                        index=0,
                        key=f"ew_{runner_id}",
                        label_visibility="collapsed"
                    )

                with col7:
                    if already_selected:
                        if st.button("Unselect", key=f"unselect_{runner_id}", use_container_width=True):
                            remove_selection(runner_id, user_id=user_id)
                            st.cache_data.clear()
                            st.rerun()
                    else:
                        if st.button("Select", key=f"select_{runner_id}", use_container_width=True):
                            is_valid, taken_odds, taken_odds_dec = validate_taken_odds(taken_odds_input)

                            if not is_valid:
                                st.warning("Taken odds must be like 9/1, 10/3, or evens.")
                            else:
                                if taken_odds is None and source_odds_normalised is not None:
                                    taken_odds = source_odds_normalised
                                    taken_odds_dec = fractional_to_decimal(taken_odds)

                                ok, message = insert_selection(
                                    row=row,
                                    user_id=user_id,
                                    user_email=CURRENT_USER_EMAIL,
                                    user_name=CURRENT_USER_NAME,
                                    stake=horse_stake,
                                    taken_odds=taken_odds,
                                    taken_odds_dec=taken_odds_dec,
                                    ew_bet=ew_label_to_flag(bet_type)
                                )
                                if ok:
                                    st.success(f"Saved selection: {row['HorseName']}")
                                else:
                                    st.warning(message)
                                st.cache_data.clear()
                                st.rerun()