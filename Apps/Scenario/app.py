import re
from pathlib import Path
import pandas as pd
import streamlit as st
import plotly.express as px

from data.bigquery_functions import get_scenario_base
from scenario_engine import apply_strategy


GREEN = "#4B5942"  # theme green
TOKENS = ["CD", "C", "D", "BF"]


def inject_css():
    css_path = Path("static/styles.css")
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)

    # Force main page text/metrics to green (keeps layout but fixes white text)
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


def parse_tokens(val: str) -> set[str]:
    """
    "| BF | | CD" -> {"BF","CD"}
    """
    if val is None:
        return set()
    parts = [p.strip().upper() for p in str(val).split("|")]
    parts = [p for p in parts if p]
    return set(parts)


def display_tokens(val: str) -> str:
    """
    "| BF | | CD" -> "BF & CD" (fixed order)
    """
    toks = parse_tokens(val)
    ordered = [t for t in TOKENS if t in toks]
    return " & ".join(ordered)


def favourite_yn(x) -> str:
    return "Y" if str(x).strip().lower() == "f" else "N"


def result_position_display(x) -> str:
    try:
        xi = int(float(x))
        return "DNF" if xi == 999 else str(xi)
    except Exception:
        return ""


def fmt_gbp0(x) -> str:
    try:
        return f"£{float(x):,.0f}"
    except Exception:
        return ""


def fmt_gbp2(x) -> str:
    try:
        return f"£{float(x):,.2f}"
    except Exception:
        return ""


def fmt_pct1(x: float) -> str:
    return f"{x*100:.1f}%"


# ------------------------
# App config
# ------------------------
st.set_page_config(page_title="Pacey32 Scenario Explorer", page_icon="🏇", layout="wide")
inject_css()
st.title("Pacey32 Scenario Explorer")


# ------------------------
# Sidebar controls
# ------------------------
with st.sidebar:
    st.subheader("Data range")
    today = pd.Timestamp.today().date()
    date_from = st.date_input("From", value=today - pd.Timedelta(days=30))
    date_to = st.date_input("To", value=today)

    st.divider()
    st.subheader("Runner filters")

    st.caption("Form flags (RaceHistoryStats)")
    must_have = st.multiselect(
        "Must include (AND)",
        options=TOKENS,
        default=[],
        help="Selected tokens must all be present in RaceHistoryStats.",
    )
    must_not_have = st.multiselect(
        "Must NOT include",
        options=TOKENS,
        default=[],
        help="Selected tokens must not be present in RaceHistoryStats.",
    )
    only_exact = st.checkbox(
        "Only exactly these flags (no extra flags)",
        value=False,
        help="If enabled, RaceHistoryStats must contain exactly the 'Must include' set and nothing else.",
    )

    st.caption("Other flags")
    f_favourite = st.checkbox(
        "Favourite",
        value=False,
        help='Matches BigQuery column Favourite = "f".',
    )

    st.caption("Odds filters (fractional, e.g. 1.0=Evens)")
    odds_min = st.number_input("Min odds", min_value=0.0, value=0.0, step=0.5)
    odds_max = st.number_input("Max odds", min_value=0.0, value=50.0, step=0.5)

    st.divider()
    st.subheader("Bet settings")
    stake = st.number_input("Stake", min_value=0.0, value=10.0, step=1.0)
    each_way = st.toggle("Each-way", value=False)
    stake_mode = st.selectbox("Stake mode", ["Per horse", "Per race"], index=0)
    st.caption("Per race = splits stake across all qualifying runners in each race.")


# ------------------------
# Load data
# ------------------------
df = get_scenario_base(date_from, date_to)

if df.empty:
    st.warning("No rows returned for the selected date range.")
    st.stop()

# Normalise expected fields
if "RaceHistoryStats" not in df.columns:
    df["RaceHistoryStats"] = ""
if "Favourite" not in df.columns:
    df["Favourite"] = ""

# Ensure datetime/date fields
if "RaceDateTime" in df.columns:
    df["RaceDateTime"] = pd.to_datetime(df["RaceDateTime"], errors="coerce")
else:
    st.error("Missing required column: RaceDateTime")
    st.stop()

# RaceDate used for chart grouping
if "RaceDateDt" in df.columns:
    # RaceDateDt often already a YYYY-MM-DD string; normalise to date
    df["RaceDate"] = pd.to_datetime(df["RaceDateDt"], errors="coerce").dt.date
elif "RaceDate" in df.columns:
    df["RaceDate"] = pd.to_datetime(df["RaceDate"], errors="coerce").dt.date
else:
    df["RaceDate"] = df["RaceDateTime"].dt.date

# Ensure odds numeric exists for filtering/strategy
if "Odds_dec" not in df.columns:
    if "Odds" in df.columns:
        # Best-effort parse fractional strings to fractional-decimal (e.g. "9/2" -> 4.5)
        def _odds_to_dec(x):
            if x is None:
                return float("nan")
            s = str(x).strip().lower()
            s = s.replace("evens", "1/1").replace("even", "1/1").replace("evs", "1/1")
            if "/" in s:
                parts = s.split("/")
                if len(parts) == 2:
                    try:
                        return float(parts[0].strip()) / float(parts[1].strip())
                    except Exception:
                        return float("nan")
            try:
                return float(s)
            except Exception:
                return float("nan")

        df["Odds_dec"] = df["Odds"].map(_odds_to_dec)
    else:
        df["Odds_dec"] = float("nan")


# ------------------------
# Apply filters
# ------------------------
mask = pd.Series(True, index=df.index)

token_sets = df["RaceHistoryStats"].map(parse_tokens)

for t in must_have:
    mask &= token_sets.map(lambda s: t in s)

for t in must_not_have:
    mask &= token_sets.map(lambda s: t not in s)

if only_exact:
    must_set = set(must_have)
    mask &= token_sets.map(lambda s: s == must_set)

if f_favourite:
    mask &= df["Favourite"].astype(str).str.lower().eq("f")

mask &= df["Odds_dec"].fillna(-1).between(odds_min, odds_max)

filt = df.loc[mask].copy()

if filt.empty:
    st.info("No runners match the current filters.")
    st.stop()


# ------------------------
# Simulate bets
# ------------------------
sim = apply_strategy(filt, stake=stake, each_way=each_way, stake_mode=stake_mode)

# Ensure fields for tooltip
sim["RaceTime"] = sim.get("RaceTime", "").astype(str)
sim["RaceLocation"] = sim.get("RaceLocation", "").astype(str)

# ------------------------
# Top visual: CUMULATIVE Profit over time with hover details
# ------------------------
def _fmt_profit(x):
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return "NA"

sim["_hover_line"] = sim.apply(
    lambda r: f"{r['RaceTime']} | {r['RaceLocation']} | {r['HorseName']} | Profit: {_fmt_profit(r['Profit'])}",
    axis=1,
)

MAX_LINES = 20
daily = (
    sim.groupby("RaceDate", dropna=False)
    .agg(
        DailyProfit=("Profit", "sum"),
        Details=("_hover_line", lambda s: "<br>".join(list(s)[:MAX_LINES]) + ("" if len(s) <= MAX_LINES else "<br>…")),
    )
    .reset_index()
)

daily["RaceDate"] = pd.to_datetime(daily["RaceDate"], errors="coerce")
daily = daily.sort_values("RaceDate")
daily["CumulativeProfit"] = daily["DailyProfit"].cumsum()

fig = px.line(
    daily,
    x="RaceDate",
    y="CumulativeProfit",
    markers=True,
    title="Cumulative profit over time",
    custom_data=["Details", "DailyProfit"],
)

fig.update_traces(
    hovertemplate=(
        "<b>%{x|%Y-%m-%d}</b>"
        "<br>Daily Profit: %{customdata[1]:.2f}"
        "<br>Cumulative Profit: %{y:.2f}"
        "<br><br>%{customdata[0]}"
        "<extra></extra>"
    )
)

# Prominent y=0 line; subtle grids
fig.add_hline(y=0, line_width=3, line_color=GREEN, opacity=0.9)

fig.update_layout(
    title_font_color=GREEN,
    xaxis_title="Race date",
    yaxis_title="Cumulative profit",
    yaxis=dict(zeroline=False, showgrid=True, gridcolor="rgba(0,0,0,0.08)"),
    xaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.06)"),
)

st.plotly_chart(fig, use_container_width=True)


# ------------------------
# Performance summary and selection stats (formatted)
# ------------------------
total_staked = float(sim["Staked"].fillna(0).sum())
total_returns = float(sim["Total_Returns"].fillna(0).sum())
profit = total_returns - total_staked
roi = (profit / total_staked) if total_staked > 0 else 0.0
strike_rate = (sim["Result_Position"].astype(str).str.strip().eq("1").mean()) if len(sim) else 0.0

race_key_cols = [c for c in ["RaceDateTime", "RaceLocation", "RaceTime"] if c in sim.columns]
races_represented = sim[race_key_cols].drop_duplicates().shape[0] if race_key_cols else 0

st.markdown("### Performance summary and selection stats")
m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
m1.metric("Horses selected", f"{len(sim):,}")
m2.metric("Races represented", f"{races_represented:,}")
m3.metric("Total staked", fmt_gbp0(total_staked))
m4.metric("Returned", fmt_gbp0(total_returns))
m5.metric("Profit", fmt_gbp2(profit))
m6.metric("ROI", fmt_pct1(roi))
m7.metric("Strike Rate", fmt_pct1(strike_rate))


# ------------------------
# Selections table (formatted display)
# ------------------------
st.markdown("### Selections")

tbl = sim.copy()

# RaceDate: use RaceDateDt if present, else derive from RaceDateTime
if "RaceDateDt" in tbl.columns:
    tbl["RaceDate"] = tbl["RaceDateDt"].astype(str)
else:
    tbl["RaceDate"] = pd.to_datetime(tbl["RaceDateTime"], errors="coerce").dt.date.astype(str)

# Odds: show Odds if present, else fall back to Odds_dec
if "Odds" in tbl.columns:
    tbl["Odds"] = tbl["Odds"].astype(str)
else:
    tbl["Odds"] = tbl["Odds_dec"].map(lambda v: "" if pd.isna(v) else f"{float(v):g}")

# Favourite as Y/N
tbl["Favourite"] = tbl.get("Favourite", "").map(favourite_yn)

# Result Position display with DNF
tbl["Result Position"] = tbl.get("Result_Position", "").map(result_position_display)

# Clean RaceHistoryStats for display
tbl["RaceHistoryStats"] = tbl.get("RaceHistoryStats", "").map(display_tokens)

# Currency formatting
tbl["Staked"] = tbl.get("Staked", "").map(fmt_gbp0)
tbl["Win Returns"] = tbl.get("Win_Returns", "").map(fmt_gbp0)
tbl["Place Returns"] = tbl.get("Place_Returns", "").map(fmt_gbp0)
tbl["Total Returns"] = tbl.get("Total_Returns", "").map(fmt_gbp0)
tbl["Profit"] = tbl.get("Profit", "").map(fmt_gbp2)

display_cols = [
    "RaceDate",
    "RaceLocation",
    "RaceTime",
    "HorseName",
    "Odds",
    "Favourite",
    "Result Position",
    "RaceHistoryStats",
    "Staked",
    "Win Returns",
    "Place Returns",
    "Total Returns",
    "Profit",
]
display_cols = [c for c in display_cols if c in tbl.columns]

st.dataframe(
    tbl[display_cols].sort_values(["RaceDate", "RaceLocation", "RaceTime", "HorseName"], ascending=True),
    use_container_width=True,
    height=650,
)