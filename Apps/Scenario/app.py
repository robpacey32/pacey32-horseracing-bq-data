import re
from pathlib import Path
import numpy as np
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


def is_true_like(x) -> bool:
    """
    Treat common boolean-ish values as True.
    Handles: True, 1, '1', 'Y', 'YES', 'TRUE', 'T'
    """
    if pd.isna(x):
        return False
    s = str(x).strip().upper()
    return s in {"TRUE", "T", "Y", "YES", "1"}


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

    st.caption("Runner requirements")
    f_cd = st.checkbox("CD", value=False)
    f_c = st.checkbox("C", value=False)
    f_d = st.checkbox("D", value=False)
    f_bf = st.checkbox("BF", value=False)
    f_favourite = st.checkbox("Favourite", value=False)

    st.caption("Recent performance")
    f_won_last_3 = st.checkbox("Won in last 3", value=False)
    f_win_2_last_6 = st.checkbox("Win 2 in last 6", value=False)

    st.caption("Race-level requirements")
    f_only_one_qualifier_in_race = st.checkbox(
        "Only 1 horse in race meets requirement",
        value=False,
        help="After applying the selected runner filters, keep only races where exactly one horse qualifies.",
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
    df["RaceDate"] = pd.to_datetime(df["RaceDateDt"], errors="coerce").dt.date
elif "RaceDate" in df.columns:
    df["RaceDate"] = pd.to_datetime(df["RaceDate"], errors="coerce").dt.date
else:
    df["RaceDate"] = df["RaceDateTime"].dt.date

# Ensure odds numeric exists for filtering/strategy
if "Odds_dec" not in df.columns:
    if "Odds" in df.columns:
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

if f_cd:
    mask &= token_sets.map(lambda s: "CD" in s)

if f_c:
    mask &= token_sets.map(lambda s: "C" in s)

if f_d:
    mask &= token_sets.map(lambda s: "D" in s)

if f_bf:
    mask &= token_sets.map(lambda s: "BF" in s)

if f_favourite:
    mask &= df["Favourite"].astype(str).str.lower().eq("f")

if f_won_last_3:
    if "WonInLast3" in df.columns:
        mask &= df["WonInLast3"].map(is_true_like)
    else:
        st.warning("Column 'WonInLast3' not found in scenario base data.")

if f_win_2_last_6:
    if "Win2InLast6" in df.columns:
        mask &= df["Win2InLast6"].map(is_true_like)
    else:
        st.warning("Column 'Win2InLast6' not found in scenario base data.")

mask &= df["Odds_dec"].fillna(-1).between(odds_min, odds_max)

filt = df.loc[mask].copy()

if filt.empty:
    st.info("No runners match the current filters.")
    st.stop()

if f_only_one_qualifier_in_race:
    race_group_cols = [c for c in ["RaceDateTime", "RaceLocation", "RaceTime"] if c in filt.columns]

    if len(race_group_cols) == 3:
        qualifier_counts = (
            filt.groupby(race_group_cols)["HorseName"]
            .size()
            .reset_index(name="qualifier_count")
        )

        filt = filt.merge(qualifier_counts, on=race_group_cols, how="left")
        filt = filt.loc[filt["qualifier_count"] == 1].copy()
        filt = filt.drop(columns=["qualifier_count"], errors="ignore")
    else:
        st.warning("Could not apply 'Only 1 horse in race meets requirement' because race grouping columns are missing.")

    if filt.empty:
        st.info("No runners match the current filters after race-level filtering.")
        st.stop()


# ------------------------
# Simulate bets
# ------------------------
sim = apply_strategy(filt, stake=stake, each_way=each_way, stake_mode=stake_mode)

if "RaceTime" not in sim.columns:
    sim["RaceTime"] = ""
if "RaceLocation" not in sim.columns:
    sim["RaceLocation"] = ""
if "HorseName" not in sim.columns:
    sim["HorseName"] = ""

sim["RaceTime"] = sim["RaceTime"].astype(str)
sim["RaceLocation"] = sim["RaceLocation"].astype(str)
sim["HorseName"] = sim["HorseName"].astype(str)

for col in ["Staked", "Win_Returns", "Place_Returns", "Total_Returns", "Profit"]:
    if col not in sim.columns:
        sim[col] = np.nan
    sim[col] = pd.to_numeric(sim[col], errors="coerce")

sim["Total_Returns_Calc"] = np.where(
    sim["Win_Returns"].notna() | sim["Place_Returns"].notna(),
    sim["Win_Returns"].fillna(0) + sim["Place_Returns"].fillna(0),
    sim["Total_Returns"]
)

sim["Profit_Calc"] = np.where(
    sim["Total_Returns_Calc"].notna() & sim["Staked"].notna(),
    sim["Total_Returns_Calc"] - sim["Staked"],
    sim["Profit"]
)

chart_view = st.selectbox(
    "Chart view",
    ["Both", "Cumulative", "Non-Cumulative"],
    index=0,
)


# ------------------------
# Top visual: Profit over time with hover details
# ------------------------
def _fmt_profit(x):
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return "NA"


sim["_hover_line"] = sim.apply(
    lambda r: f"{r['RaceTime']} | {r['RaceLocation']} | {r['HorseName']} | Profit: {_fmt_profit(r['Profit_Calc'])}",
    axis=1,
)

MAX_LINES = 20
daily = (
    sim.groupby("RaceDate", dropna=False)
    .agg(
        DailyProfit=("Profit_Calc", "sum"),
        Details=("_hover_line", lambda s: "<br>".join(list(s)[:MAX_LINES]) + ("" if len(s) <= MAX_LINES else "<br>…")),
    )
    .reset_index()
)

daily["RaceDate"] = pd.to_datetime(daily["RaceDate"], errors="coerce")
daily = daily.sort_values("RaceDate")
daily["CumulativeProfit"] = daily["DailyProfit"].cumsum()

if chart_view == "Cumulative":
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
    yaxis_title = "Cumulative profit"

elif chart_view == "Non-Cumulative":
    fig = px.bar(
        daily,
        x="RaceDate",
        y="DailyProfit",
        title="Daily profit over time",
        custom_data=["Details"],
    )

    fig.update_traces(
        hovertemplate=(
            "<b>%{x|%Y-%m-%d}</b>"
            "<br>Daily Profit: %{y:.2f}"
            "<br><br>%{customdata[0]}"
            "<extra></extra>"
        )
    )
    yaxis_title = "Daily profit"

else:
    fig = px.line(
        daily,
        x="RaceDate",
        y=["CumulativeProfit", "DailyProfit"],
        markers=True,
        title="Profit over time",
        custom_data=["Details"],
    )

    fig.update_traces(
        hovertemplate=(
            "<b>%{x|%Y-%m-%d}</b>"
            "<br>%{fullData.name}: %{y:.2f}"
            "<br><br>%{customdata[0]}"
            "<extra></extra>"
        )
    )
    yaxis_title = "Profit"

fig.add_hline(y=0, line_width=3, line_color=GREEN, opacity=0.9)

fig.update_layout(
    title_font_color=GREEN,
    xaxis_title="Race date",
    yaxis_title=yaxis_title,
    yaxis=dict(zeroline=False, showgrid=True, gridcolor="rgba(0,0,0,0.08)"),
    xaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.06)"),
)

st.plotly_chart(fig, use_container_width=True)


# ------------------------
# Performance summary and selection stats (formatted)
# ------------------------
total_staked = float(sim["Staked"].fillna(0).sum())
total_returns = float(sim["Total_Returns_Calc"].fillna(0).sum())
profit = float(sim["Profit_Calc"].fillna(0).sum())
roi = (profit / total_staked) if total_staked > 0 else 0.0
strike_rate = (sim["Result_Position"].astype(str).str.strip().eq("1").mean()) if len(sim) else 0.0

race_key_cols = [c for c in ["RaceDateTime", "RaceLocation", "RaceTime"] if c in sim.columns]
races_represented = sim[race_key_cols].drop_duplicates().shape[0] if race_key_cols else 0

st.markdown("### Performance summary and selection stats")

sp1 = st.columns([0.3, 1, 1, 1, 1, 0.3], gap="small")
sp1[1].metric("Horses selected", f"{len(sim):,}")
sp1[2].metric("Races represented", f"{races_represented:,}")
sp1[3].metric("Total staked", fmt_gbp0(total_staked))
sp1[4].metric("Returned", fmt_gbp0(total_returns))

sp2 = st.columns([0.8, 1, 1, 1, 0.8], gap="small")
sp2[1].metric("Profit", fmt_gbp2(profit))
sp2[2].metric("ROI", fmt_pct1(roi))
sp2[3].metric("Strike Rate", fmt_pct1(strike_rate))


# ------------------------
# Selections table (formatted display)
# ------------------------
st.markdown("### Selections")

tbl = sim.copy()

if "RaceDateDt" in tbl.columns:
    tbl["RaceDate"] = tbl["RaceDateDt"].astype(str)
else:
    tbl["RaceDate"] = pd.to_datetime(tbl["RaceDateTime"], errors="coerce").dt.date.astype(str)

if "Odds" in tbl.columns:
    tbl["Odds"] = tbl["Odds"].astype(str)
else:
    tbl["Odds"] = tbl["Odds_dec"].map(lambda v: "" if pd.isna(v) else f"{float(v):g}")

tbl["Favourite"] = tbl.get("Favourite", "").map(favourite_yn)
tbl["Result Position"] = tbl.get("Result_Position", "").map(result_position_display)
tbl["RaceHistoryStats"] = tbl.get("RaceHistoryStats", "").map(display_tokens)

tbl["Staked"] = tbl.get("Staked", "").map(fmt_gbp0)
tbl["Win Returns"] = tbl.get("Win_Returns", "").map(fmt_gbp0)
tbl["Place Returns"] = tbl.get("Place_Returns", "").map(fmt_gbp0)
tbl["Total Returns"] = tbl.get("Total_Returns_Calc", "").map(fmt_gbp0)
tbl["Profit"] = tbl.get("Profit_Calc", "").map(fmt_gbp2)

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