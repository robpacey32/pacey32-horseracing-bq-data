from pathlib import Path
import sys
import re
import warnings

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pymongo import MongoClient
from google.cloud import bigquery
from google.oauth2 import service_account

warnings.filterwarnings(
    "ignore",
    message="The behavior of DatetimeProperties.to_pydatetime is deprecated"
)

# -------------------------
# PAGE CONFIG
# -------------------------
st.set_page_config(page_title="Historic Performance", layout="wide")

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
# CONFIG
# -------------------------
cfg = get_config()

KEY_PATH = cfg["KEY_PATH"]
PROJECT_ID = cfg["PROJECT_ID"]
VIEW_ID = cfg.get(
    "BQ_HISTORIC_VIEW_ID",
    "horseracing-pacey32-github.bettingapp.3_BetSelections_Enriched",
)

MONGO_URI = cfg["MONGO_URI"]
MONGO_DB_NAME = cfg.get("APP_MONGO_DB_NAME", "bettingapp")
MONGO_COLLECTION_NAME = cfg.get("APP_MONGO_COLLECTION_NAME", "bet_selections")

GREEN_COLOUR = "#8FA58C"
AMBER_COLOUR = "#D8B26E"

# -------------------------
# CSS / THEME
# -------------------------
def apply_bettracker_theme():
    css_path = Path(__file__).resolve().parents[1] / "styles.css"
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

CURRENT_USER_ID = str(user["user_id"])

with st.sidebar:
    st.write(f"Signed in as: {user.get('email', 'Unknown user')}")
    if st.button("Log out"):
        logout()
        st.rerun()

# -------------------------
# HELPERS
# -------------------------
def normalise_odds_input(odds_value):
    if odds_value is None or pd.isna(odds_value):
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


def fractional_to_decimal(odds_value):
    normalised = normalise_odds_input(odds_value)

    if normalised is None:
        return None

    if normalised == "evens":
        return 2.0

    num, den = normalised.split("/")
    return (float(num) / float(den)) + 1.0


def normalise_bet_type(value):
    if value is None or pd.isna(value):
        return "Win"

    clean = str(value).strip().upper()
    return "EW" if clean == "EW" else "Win"


def get_effective_odds_dec(row):
    taken_odds_dec = row.get("taken_odds_dec")
    if pd.notna(taken_odds_dec):
        try:
            return float(taken_odds_dec)
        except Exception:
            pass

    taken_odds = row.get("taken_odds")
    if pd.notna(taken_odds):
        taken_dec = fractional_to_decimal(taken_odds)
        if taken_dec is not None:
            return taken_dec

    source_odds = row.get("odds")
    if pd.notna(source_odds):
        source_dec = fractional_to_decimal(source_odds)
        if source_dec is not None:
            return source_dec

    return None


def infer_place_terms_from_source(row):
    source_odds_dec = fractional_to_decimal(row.get("odds"))
    place_returns_multiple = pd.to_numeric(row.get("Place_Returns"), errors="coerce")

    if source_odds_dec is None or source_odds_dec <= 1:
        return None

    if pd.isna(place_returns_multiple) or place_returns_multiple <= 0:
        return None

    return max((place_returns_multiple - 1.0) / (source_odds_dec - 1.0), 0.0)


def calc_taken_win_return(row):
    odds_dec = get_effective_odds_dec(row)
    stake = float(row.get("stake", 0) or 0)
    result_pos = pd.to_numeric(row.get("Result_Position"), errors="coerce")

    if odds_dec is None or pd.isna(result_pos):
        return 0.0

    return stake * odds_dec if result_pos == 1 else 0.0


def calc_taken_place_return(row):
    bet_type = normalise_bet_type(row.get("bet_type"))
    if bet_type != "EW":
        return 0.0

    odds_dec = get_effective_odds_dec(row)
    stake = float(row.get("stake", 0) or 0)
    place_returns_multiple = pd.to_numeric(row.get("Place_Returns"), errors="coerce")

    if odds_dec is None or pd.isna(place_returns_multiple) or place_returns_multiple <= 0:
        return 0.0

    place_terms = infer_place_terms_from_source(row)
    if place_terms is None:
        return 0.0

    adjusted_place_multiple = 1.0 + ((odds_dec - 1.0) * place_terms)
    return stake * adjusted_place_multiple


def calc_taken_total_return(row):
    return calc_taken_win_return(row) + calc_taken_place_return(row)


def calc_selected_return(row, odds_basis):
    bet_type = normalise_bet_type(row.get("bet_type"))

    if odds_basis == "Data odds":
        if bet_type == "EW":
            return float(row.get("Actual_Total_Return", 0) or 0)
        return float(row.get("Actual_Win_Return", 0) or 0)

    if bet_type == "EW":
        return calc_taken_total_return(row)
    return calc_taken_win_return(row)


def calc_effective_staked(row):
    stake = float(row.get("stake", 0) or 0)
    bet_type = normalise_bet_type(row.get("bet_type"))
    return stake * 2 if bet_type == "EW" else stake


def format_currency(value):
    try:
        return f"£{float(value):,.2f}"
    except Exception:
        return "£0.00"


def profit_colour(val):
    try:
        numeric_val = float(val)
    except Exception:
        return ""

    if numeric_val < 0:
        return "color: #B00020; font-weight: 600;"
    if numeric_val > 0:
        return "color: #2E7D32; font-weight: 600;"
    return ""


# -------------------------
# BIGQUERY
# -------------------------
@st.cache_resource
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

bq_client = get_bigquery_client()

# -------------------------
# MONGODB
# -------------------------
@st.cache_resource
def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    return db[MONGO_COLLECTION_NAME]

mongo_collection = get_mongo_collection()

# -------------------------
# DATA LOADERS
# -------------------------
@st.cache_data(ttl=60)
def load_bet_data(user_id: str):
    query = f"""
        SELECT *
        FROM `{VIEW_ID}`
        WHERE user_id = @user_id
        ORDER BY race_date DESC, race_time
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
        ]
    )

    df = bq_client.query(query, job_config=job_config).to_dataframe(
        create_bqstorage_client=False
    )

    if df.empty:
        return df

    if "race_date" in df.columns:
        df["race_date"] = pd.to_datetime(df["race_date"]).dt.date

    numeric_cols = [
        "stake",
        "Actual_Win_Return",
        "Actual_Place_Return",
        "Actual_Total_Return",
        "Place_Returns",
        "Result_Position",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = 0.0

    fill_zero_cols = [
        "stake",
        "Actual_Win_Return",
        "Actual_Place_Return",
        "Actual_Total_Return",
        "Place_Returns",
    ]

    for col in fill_zero_cols:
        df[col] = df[col].fillna(0.0)

    for col in ["race_time", "race_location", "horse_name", "odds"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
        else:
            df[col] = ""

    if "runner_id" in df.columns:
        df["runner_id"] = df["runner_id"].astype(str)

    return df


@st.cache_data(ttl=60)
def load_bet_meta_map(user_id: str):
    docs = list(
        mongo_collection.find(
            {"user_id": user_id},
            {
                "_id": 0,
                "runner_id": 1,
                "taken_odds": 1,
                "taken_odds_dec": 1,
                "bet_type": 1,
                "ew_bet": 1,
            }
        )
    )

    if not docs:
        return pd.DataFrame(columns=["runner_id", "taken_odds", "taken_odds_dec", "bet_type"])

    df = pd.DataFrame(docs)
    df["runner_id"] = df["runner_id"].astype(str)

    if "bet_type" not in df.columns:
        if "ew_bet" in df.columns:
            df["bet_type"] = df["ew_bet"].apply(
                lambda x: "EW" if str(x).strip().upper() in {"Y", "EW"} else "Win"
            )
        else:
            df["bet_type"] = "Win"

    df["bet_type"] = df["bet_type"].apply(normalise_bet_type)
    return df[["runner_id", "taken_odds", "taken_odds_dec", "bet_type"]]

# -------------------------
# APP
# -------------------------
st.title("Historic Performance")

df = load_bet_data(CURRENT_USER_ID)

if df.empty:
    st.warning("No performance data found.")
    st.stop()

bet_meta_df = load_bet_meta_map(CURRENT_USER_ID)

if not bet_meta_df.empty:
    df = df.merge(bet_meta_df, on="runner_id", how="left")
else:
    df["taken_odds"] = None
    df["taken_odds_dec"] = None
    df["bet_type"] = "Win"

df["bet_type"] = df["bet_type"].apply(normalise_bet_type)
df["effective_staked"] = df.apply(calc_effective_staked, axis=1)

# -------------------------
# FILTERS
# -------------------------
metric_options = [
    "# Horses",
    "£ Staked",
    "£ Return",
    "£ Profit",
    "Cumulative £ Profit",
    "# Wins",
    "# Wins & Places",
]

col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    selected_metric_label = st.selectbox("Metric", metric_options)

with col2:
    bet_type_filter = st.selectbox("Bet Type", ["All", "Win", "EW"])

with col3:
    odds_basis = st.selectbox("Odds Used", ["Data odds", "Taken odds"])

if bet_type_filter == "Win":
    df = df[df["bet_type"] == "Win"].copy()
elif bet_type_filter == "EW":
    df = df[df["bet_type"] == "EW"].copy()

if df.empty:
    st.warning("No performance data found for the selected filters.")
    st.stop()

# -------------------------
# RETURN LOGIC
# -------------------------
df["selected_return_row"] = df.apply(lambda row: calc_selected_return(row, odds_basis), axis=1)

# -------------------------
# DAILY SUMMARY
# -------------------------
daily_summary = (
    df.groupby("race_date", dropna=False)
    .agg(
        horses=("runner_id", "count"),
        total_staked=("effective_staked", "sum"),
        selected_return=("selected_return_row", "sum"),
        wins=("Actual_Win_Return", lambda s: (pd.to_numeric(s, errors="coerce").fillna(0) > 0).sum()),
        wins_places=("Place_Returns", lambda s: (pd.to_numeric(s, errors="coerce").fillna(0) > 0).sum()),
    )
    .reset_index()
    .sort_values("race_date")
)

daily_summary["profit"] = daily_summary["selected_return"] - daily_summary["total_staked"]
daily_summary["cumulative_profit"] = daily_summary["profit"].cumsum()

# -------------------------
# METRIC MAP
# -------------------------
metric_map = {
    "# Horses": ("horses", "# Horses"),
    "£ Staked": ("total_staked", "£ Staked"),
    "£ Return": ("selected_return", "£ Return"),
    "£ Profit": ("profit", "£ Profit"),
    "Cumulative £ Profit": ("cumulative_profit", "Cumulative £ Profit"),
    "# Wins": ("wins", "# Wins"),
    "# Wins & Places": ("wins_places", "# Wins & Places"),
}

selected_metric, selected_title = metric_map[selected_metric_label]

# -------------------------
# TOTALS TABLE
# -------------------------
totals_df = pd.DataFrame(
    {
        "Metric": [
            "# Horses",
            "£ Staked",
            "£ Return",
            "£ Profit",
            "Cumulative £ Profit",
            "# Wins",
            "# Wins & Places",
        ],
        "Value": [
            int(daily_summary["horses"].sum()),
            round(daily_summary["total_staked"].sum(), 2),
            round(daily_summary["selected_return"].sum(), 2),
            round(daily_summary["profit"].sum(), 2),
            round(daily_summary["cumulative_profit"].iloc[-1], 2) if not daily_summary.empty else 0.0,
            int(daily_summary["wins"].sum()),
            int(daily_summary["wins_places"].sum()),
        ],
    }
)

currency_metric_names = {"£ Staked", "£ Return", "£ Profit", "Cumulative £ Profit"}
totals_display_df = totals_df.copy()
totals_display_df["Value"] = totals_display_df.apply(
    lambda row: format_currency(row["Value"]) if row["Metric"] in currency_metric_names else f"{int(row['Value'])}",
    axis=1
)

# -------------------------
# HOVER TEXT FOR PLOT
# -------------------------
hover_per_day = (
    df.sort_values(["race_date", "race_time", "race_location", "horse_name"])
      .groupby("race_date", dropna=False)
      .apply(
          lambda x: "<br>".join(
              x.apply(
                  lambda r: f"{r['race_time']} | {r['race_location']} | {r['horse_name']}",
                  axis=1
              ).tolist()
          ),
          include_groups=False
      )
      .reset_index(name="hover_text")
)

chart_df = daily_summary.merge(hover_per_day, on="race_date", how="left")
chart_df["race_date"] = pd.to_datetime(chart_df["race_date"])

# -------------------------
# LAYOUT
# -------------------------
left_col, right_col = st.columns([2, 1])

with left_col:
    st.markdown("### Performance Over Time")

    if selected_metric in ["profit", "cumulative_profit"]:
        bar_colours = [
            AMBER_COLOUR if value < 0 else GREEN_COLOUR
            for value in chart_df[selected_metric]
        ]
    else:
        bar_colours = [GREEN_COLOUR] * len(chart_df)

    fig = go.Figure(
        data=[
            go.Bar(
                x=chart_df["race_date"],
                y=chart_df[selected_metric],
                marker_color=bar_colours,
                customdata=chart_df["hover_text"],
                hovertemplate=(
                    "<b>%{x|%d %b %Y}</b><br><br>"
                    "%{customdata}<extra></extra>"
                ),
            )
        ]
    )

    fig.update_layout(
        height=400,
        xaxis_title="Race Date",
        yaxis_title=selected_title,
        hoverlabel=dict(
            bgcolor="white",
            font_size=13,
            font_family="Inter"
        ),
        showlegend=False,
        bargap=0.15,
    )

    fig.update_xaxes(
        tickformat="%d %b %Y",
        tickangle=-35
    )

    st.plotly_chart(fig, width="stretch")

with right_col:
    st.markdown("### Totals")
    st.dataframe(
        totals_display_df,
        width="stretch",
        hide_index=True
    )

# -------------------------
# DAILY SUMMARY TABLE
# -------------------------
st.markdown("<hr>", unsafe_allow_html=True)
st.markdown("### Daily Summary")

display_daily = daily_summary.copy().rename(
    columns={
        "race_date": "Race Date",
        "horses": "# Horses",
        "total_staked": "£ Staked",
        "selected_return": "£ Return",
        "profit": "£ Profit",
        "cumulative_profit": "Cumulative £ Profit",
        "wins": "# Wins",
        "wins_places": "# Wins & Places",
    }
)

display_daily = display_daily.sort_values("Race Date", ascending=False)

styled_daily = (
    display_daily.style
    .format({
        "£ Staked": lambda v: format_currency(v),
        "£ Return": lambda v: format_currency(v),
        "£ Profit": lambda v: format_currency(v),
        "Cumulative £ Profit": lambda v: format_currency(v),
    })
    .map(profit_colour, subset=["£ Profit", "Cumulative £ Profit"])
)

st.dataframe(
    styled_daily,
    width="stretch",
    hide_index=True
)