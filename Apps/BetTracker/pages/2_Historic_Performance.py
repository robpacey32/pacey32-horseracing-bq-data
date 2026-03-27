from pathlib import Path
import sys

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
import numpy as np
import warnings

warnings.filterwarnings(
    "ignore",
    message="The behavior of DatetimeProperties.to_pydatetime is deprecated"
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
# PAGE CONFIG
# -------------------------
st.set_page_config(page_title="Historic Performance", layout="wide")

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
# BIGQUERY
# -------------------------
@st.cache_resource
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

bq_client = get_bigquery_client()


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
        "Place_Returns"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        else:
            df[col] = 0.0

    for col in ["race_time", "race_location", "horse_name"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
        else:
            df[col] = ""

    return df

# -------------------------
# APP
# -------------------------
st.title("Historic Performance")

df = load_bet_data(CURRENT_USER_ID)

if df.empty:
    st.warning("No performance data found.")
    st.stop()

# -------------------------
# FILTERS
# -------------------------
metric_options = [
    "# Horses",
    "£ Staked",
    "£ Return",
    "£ Profit",
    "# Wins",
    "# Wins & Places",
]

col1, col2 = st.columns([2, 1])

with col1:
    selected_metric_label = st.selectbox("Metric", metric_options)

with col2:
    ew_selector = st.selectbox("Each Way", ["N", "Y"])

# -------------------------
# RETURN LOGIC
# -------------------------
if ew_selector == "Y":
    df["selected_return_row"] = df["Actual_Total_Return"]
else:
    df["selected_return_row"] = df["Actual_Win_Return"]

# -------------------------
# DAILY SUMMARY
# -------------------------
daily_summary = (
    df.groupby("race_date", dropna=False)
    .agg(
        horses=("runner_id", "count"),
        total_staked=("stake", "sum"),
        selected_return=("selected_return_row", "sum"),
        wins=("Actual_Win_Return", lambda s: (s > 0).sum()),
        wins_places=("Place_Returns", lambda s: (s > 0).sum()),
    )
    .reset_index()
    .sort_values("race_date")
)

daily_summary["profit"] = daily_summary["selected_return"] - daily_summary["total_staked"]

# -------------------------
# METRIC MAP
# -------------------------
metric_map = {
    "# Horses": ("horses", "# Horses"),
    "£ Staked": ("total_staked", "£ Staked"),
    "£ Return": ("selected_return", "£ Return"),
    "£ Profit": ("profit", "£ Profit"),
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
            "# Wins",
            "# Wins & Places",
        ],
        "Value": [
            int(daily_summary["horses"].sum()),
            round(daily_summary["total_staked"].sum(), 2),
            round(daily_summary["selected_return"].sum(), 2),
            round(daily_summary["profit"].sum(), 2),
            int(daily_summary["wins"].sum()),
            int(daily_summary["wins_places"].sum()),
        ],
    }
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
          )
      ,include_groups=False)
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

    if selected_metric == "profit":
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
        totals_df,
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
        "wins": "# Wins",
        "wins_places": "# Wins & Places",
    }
)

st.dataframe(
    display_daily.sort_values("Race Date", ascending=False),
    width="stretch",
    hide_index=True
)