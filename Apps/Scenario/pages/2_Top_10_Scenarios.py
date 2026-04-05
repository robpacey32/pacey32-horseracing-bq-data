from pathlib import Path
import pandas as pd
import streamlit as st

from data.bigquery_functions import _get_bq_client


GREEN = "#4B5942"


def inject_css():
    css_path = Path("static/styles.css")
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)

    st.markdown(
        f"""
        <style>
          .stMarkdown, .stMarkdown p, .stMarkdown span, .stText, div, span {{
            color: {GREEN} !important;
          }}
          [data-testid="stMetricLabel"] {{
            color: {GREEN} !important;
          }}
          [data-testid="stMetricValue"] {{
            color: {GREEN} !important;
          }}
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner="Loading scenario iterations...", ttl=0)
def load_iterations():
    client = _get_bq_client()

    query = """
    WITH latest_run AS (
      SELECT MAX(RunTimestamp) AS max_run_ts
      FROM `horseracing-pacey32-github.horseraceanalytics.ScenarioIterations_GitHub`
    )
    SELECT t.*
    FROM `horseracing-pacey32-github.horseraceanalytics.ScenarioIterations_GitHub` t
    INNER JOIN latest_run r
      ON t.RunTimestamp = r.max_run_ts
    """

    df = client.query(query).result().to_dataframe()

    numeric_cols = [
        "Rank",
        "LookbackMonths",
        "Bets",
        "BetsPerDay",
        "BetsPerWeek",
        "ActiveDays",
        "PctDaysWithBet",
        "Profit",
        "ProfitPerBet",
        "ProfitPerDay",
        "ROI",
        "StrikeRate",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def fmt_gbp2(x):
    try:
        return f"£{float(x):,.2f}"
    except Exception:
        return ""


def fmt_pct1(x):
    try:
        return f"{float(x) * 100:.1f}%"
    except Exception:
        return ""


st.set_page_config(page_title="Top 10 Scenarios", page_icon="🏇", layout="wide")
inject_css()
st.title("Top 10 Scenarios")

df = load_iterations()

if df.empty:
    st.warning("No scenario iteration results found.")
    st.stop()

run_ts = df["RunTimestamp"].iloc[0] if "RunTimestamp" in df.columns else None
if run_ts is not None:
    st.caption(f"Latest run: {run_ts}")

# ------------------------
# Filters
# ------------------------
st.markdown("### Filters")

f1, f2, f3, f4 = st.columns(4)

with f1:
    min_bets = st.number_input("Min bets", min_value=0, value=0, step=1)

with f2:
    min_bets_per_week = st.number_input("Min bets per week", min_value=0.0, value=0.0, step=0.1)

with f3:
    min_roi = st.number_input("Min ROI", value=0.0, step=0.01)

with f4:
    min_profit = st.number_input("Min profit", value=0.0, step=1.0)

f5, f6, f7 = st.columns(3)

with f5:
    min_strike_rate = st.number_input("Min strike rate", min_value=0.0, max_value=1.0, value=0.0, step=0.01)

with f6:
    min_pct_days = st.number_input("Min % days with bet", min_value=0.0, max_value=1.0, value=0.0, step=0.01)

with f7:
    top_n = st.number_input("Show top N", min_value=1, max_value=100, value=10, step=1)

sort_option = st.selectbox(
    "Sort by",
    [
        "Rank",
        "ProfitPerDay",
        "ROI",
        "Profit",
        "BetsPerWeek",
        "StrikeRate",
    ],
    index=0,
)

ascending = sort_option == "Rank"

# ------------------------
# Apply filters
# ------------------------
filt = df.copy()

filt = filt.loc[filt["Bets"].fillna(0) >= min_bets]
filt = filt.loc[filt["BetsPerWeek"].fillna(0) >= min_bets_per_week]
filt = filt.loc[filt["ROI"].fillna(0) >= min_roi]
filt = filt.loc[filt["Profit"].fillna(0) >= min_profit]
filt = filt.loc[filt["StrikeRate"].fillna(0) >= min_strike_rate]
filt = filt.loc[filt["PctDaysWithBet"].fillna(0) >= min_pct_days]

filt = filt.sort_values(sort_option, ascending=ascending).head(top_n).copy()

if filt.empty:
    st.info("No scenarios match the current filters.")
    st.stop()

# ------------------------
# Display
# ------------------------
show = filt.copy()

if "Profit" in show.columns:
    show["Profit"] = show["Profit"].map(fmt_gbp2)
if "ProfitPerBet" in show.columns:
    show["ProfitPerBet"] = show["ProfitPerBet"].map(fmt_gbp2)
if "ProfitPerDay" in show.columns:
    show["ProfitPerDay"] = show["ProfitPerDay"].map(fmt_gbp2)
if "ROI" in show.columns:
    show["ROI"] = show["ROI"].map(fmt_pct1)
if "StrikeRate" in show.columns:
    show["StrikeRate"] = show["StrikeRate"].map(fmt_pct1)
if "PctDaysWithBet" in show.columns:
    show["PctDaysWithBet"] = show["PctDaysWithBet"].map(fmt_pct1)

display_cols = [
    "Rank",
    "Conditions",
    "Bets",
    "BetsPerDay",
    "BetsPerWeek",
    "ActiveDays",
    "PctDaysWithBet",
    "Profit",
    "ProfitPerBet",
    "ProfitPerDay",
    "ROI",
    "StrikeRate",
]

display_cols = [c for c in display_cols if c in show.columns]

rows = len(show)
height = min(35 * (rows + 1), 700)  # dynamic height

st.dataframe(
    show[display_cols].reset_index(drop=True),
    use_container_width=True,
    height=height,
    hide_index=True
)