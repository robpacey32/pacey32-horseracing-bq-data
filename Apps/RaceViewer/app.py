import re
import streamlit as st
import pandas as pd
from pathlib import Path

from data.bigquery_functions import (
    get_races_for_date,
    get_single_race,
    get_totals,
    get_last12m_entity_stats, 
)

# ------------------------
# CSS injection
# ------------------------
def inject_css():
    css_path = Path("static/styles.css")
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)

# ------------------------
# Column prettifier + cleaning
# ------------------------
COL_OVERRIDES = {
    "Pre_RaceClass": "Race Class",
    "Pre_RaceSurface": "Surface",
    "Pre_RaceDayOfWeek": "Day",
    "Pre_RaceGoing": "Going",
    "Pre_RaceDistance": "Distance",
    "Pre_RaceRunners": "Runners",
    "Pre_RaceTime": "Time",
    "RaceStatus": "Status",
    "Pre_RaceLocation": "Meeting",
    "HorseNumber": "No.",
    "StallNumber": "Stall",
    "HorseName": "Horse",
    "RaceHistoryStats": "Form",
    "LastRun": "Last Run",
    "PrizeMoney": "Prize Money",   # CHANGED
    "Post_Jockey": "Jockey",
    "Post_Trainer": "Trainer",
    "RideDescription": "Comment",
    "_OddsRank": "Odds Rank",
}

def prettify_col(col: str) -> str:
    if col in COL_OVERRIDES:
        return COL_OVERRIDES[col]
    col = re.sub(r"^(Pre_|PreRace_|Pre_Race)", "", col)
    col = re.sub(r"([a-z])([A-Z])", r"\1 \2", col)
    return col.replace("_", " ").strip().title()

def prettify_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={c: prettify_col(c) for c in df.columns})

def blank_na(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.replace(
            {
                "N/A": "",
                "n/a": "",
                "NA": "",
                "na": "",
                "None": "",
                None: "",
            }
        )
        .fillna("")
    )

# ------------------------
# Helpers
# ------------------------
def pos_sort_key(val):
    if pd.isna(val):
        return 999
    s = str(val).lower().strip()
    for suf in ["st", "nd", "rd", "th"]:
        if s.endswith(suf):
            s = s[:-len(suf)]
    return int(s) if s.isdigit() else 900

def to_int_safe(x):
    try:
        if x is None or x == "":
            return pd.NA
        return int(float(str(x).strip()))
    except Exception:
        return pd.NA

def odds_to_decimal(odds):
    """
    Convert fractional odds to fractional decimal value:
    '9/2' -> 4.5, '4/5' -> 0.8, '10/1' -> 10.0, 'Evens' -> 1.0
    """
    if odds is None or odds == "":
        return float("nan")
    s = str(odds).strip().lower()
    s = s.replace("evens", "1/1").replace("even", "1/1").replace("evs", "1/1")
    if "/" in s:
        parts = s.split("/")
        if len(parts) == 2:
            try:
                num = float(parts[0].strip())
                den = float(parts[1].strip())
                if den == 0:
                    return float("nan")
                return num / den
            except Exception:
                return float("nan")
    try:
        return float(s)
    except Exception:
        return float("nan")

def clean_form(val):
    """
    - If starts with '|', remove leading pipe and whitespace.
    - If multiple pipes like '| D | BF', output 'D & BF'
    - Otherwise return stripped original.
    """
    if val is None:
        return ""
    s = str(val).strip()
    if s == "":
        return ""
    if "|" not in s:
        return s

    parts = [p.strip() for p in s.split("|")]
    parts = [p for p in parts if p]  # drop empties
    if not parts:
        return ""
    return " & ".join(parts)

def format_prize_money(val, default_symbol="£"):
    """
    Formats prize money as £12,345 or €0 (no decimals).
    Uses detected symbol if present; otherwise uses default_symbol.
    """
    if val is None or val == "":
        return f"{default_symbol}0"

    s = str(val).strip()

    # Detect currency in the cell
    if "€" in s:
        symbol = "€"
    elif "£" in s:
        symbol = "£"
    else:
        symbol = default_symbol

    # Extract numeric part (keep digits and dot)
    num = re.sub(r"[^\d.]", "", s)

    try:
        amount = int(float(num)) if num else 0
        return f"{symbol}{amount:,}"
    except Exception:
        return f"{symbol}0"
    
def convert_currency(amount_gbp, target="GBP"):
    """
    amount_gbp: numeric value assumed to be GBP-equivalent
    target: 'GBP' or 'EUR'
    """
    if amount_gbp is None or pd.isna(amount_gbp):
        return 0

    if target == "EUR":
        return amount_gbp * 1.15  # £ → €
    return amount_gbp

# ------------------------
# App config
# ------------------------
st.set_page_config(
    page_title="Pacey32 Race Viewer",
    page_icon="🏇",
    layout="wide",
)

inject_css()
st.title("Pacey32 Race Viewer")

# ============================================================
# FILTER BAR
# ============================================================
initial_date = pd.Timestamp.today().date()

st.markdown('<div class="filterbar">', unsafe_allow_html=True)
c1, c2, c3 = st.columns([1.2, 2.5, 1], vertical_alignment="center")

with c1:
    selected_date = st.date_input("Date", value=initial_date, label_visibility="collapsed")

if not selected_date:
    st.stop()

races_for_day = get_races_for_date(selected_date)
if races_for_day.empty:
    st.warning("No races found on this date.")
    st.stop()

loc_options = sorted(races_for_day["Pre_RaceLocation"].dropna().unique())

with c2:
    selected_location = st.selectbox("Meeting", loc_options, label_visibility="collapsed")

time_options = (
    races_for_day[races_for_day["Pre_RaceLocation"] == selected_location]["Pre_RaceTime"]
    .dropna()
    .unique()
)
time_options = sorted(time_options)

with c3:
    selected_time = st.selectbox("Time", time_options, label_visibility="collapsed")

st.markdown("</div>", unsafe_allow_html=True)

# ============================================================
# Load selected race
# ============================================================
race_key = races_for_day[
    (races_for_day["Pre_RaceLocation"] == selected_location)
    & (races_for_day["Pre_RaceTime"] == selected_time)
]["Pre_SourceURL"].iloc[0]

df = get_single_race(race_key)
if df.empty:
    st.error("No race data returned.")
    st.stop()

st.markdown("---")
tab_pre, tab_res, tab_12m = st.tabs(["🐎 Pre Race", "🏁 Results", "📈 Last 12 Months"])

# ============================================================
# PRE-RACE TAB
# ============================================================
with tab_pre:
    # ---------------- Race info ----------------
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Race info")

    info_cols = [
        "Pre_RaceClass",
        "Pre_RaceSurface",
        "Pre_RaceDayOfWeek",
        "Pre_RaceGoing",
        "Pre_RaceDistance",
        "Pre_RaceRunners",
        "Pre_RaceTime",
        "RaceStatus",
    ]
    info_cols = [c for c in info_cols if c in df.columns]

    info_df = blank_na(prettify_df(df[info_cols].drop_duplicates()))
    st.dataframe(info_df, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # ---------------- Declared runners ----------------
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Declared runners")

    prerace_cols = [
        "HorseNumber",
        "StallNumber",
        "SilkURL",
        "HorseName",
        "Age",
        "Weight",
        "Headgear",
        "LastRun",
        "RaceHistoryStats",
        "Jockey",
        "Trainer",
        "Odds",
    ]
    prerace_cols = [c for c in prerace_cols if c in df.columns]

    prerace = blank_na(df[prerace_cols].copy())

    # numeric columns for correct sorting
    if "HorseNumber" in prerace.columns:
        prerace["HorseNumber"] = prerace["HorseNumber"].apply(to_int_safe).astype("Int64")
    if "StallNumber" in prerace.columns:
        prerace["StallNumber"] = prerace["StallNumber"].apply(to_int_safe).astype("Int64")

    # clean form formatting
    if "RaceHistoryStats" in prerace.columns:
        prerace["RaceHistoryStats"] = prerace["RaceHistoryStats"].apply(clean_form)

    # silk images (hide URL)
    if "SilkURL" in prerace.columns:
        prerace["Silk"] = prerace["SilkURL"]
        prerace = prerace.drop(columns=["SilkURL"])

    # Odds rank: decimalise -> dense rank (1,2,3...) and DEFAULT sort by it
    if "Odds" in prerace.columns:
        _od = prerace["Odds"].apply(odds_to_decimal)
        prerace["_OddsRank"] = pd.Series(_od).rank(method="dense", ascending=True).astype("Int64")

        prerace = prerace.sort_values(
            by=["_OddsRank", "HorseNumber"],
            ascending=[True, True],
            na_position="last",
            kind="mergesort",
        )

    prerace_display = prettify_df(prerace)

    desired_order = [
        "No.",
        "Stall",
        "Silk",
        "Horse",
        "Age",
        "Weight",
        "Headgear",
        "Last Run",
        "Form",
        "Jockey",
        "Trainer",
        "Odds",
        "Odds Rank",
    ]
    existing = [c for c in desired_order if c in prerace_display.columns]
    prerace_display = prerace_display[existing + [c for c in prerace_display.columns if c not in existing]]

    st.dataframe(
        prerace_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Silk": st.column_config.ImageColumn("Silk", width="small"),
            "Odds Rank": st.column_config.NumberColumn("Odds Rank", format="%d", width="small"),
            "No.": st.column_config.NumberColumn("No.", width="small"),
            "Stall": st.column_config.NumberColumn("Stall", width="small"),
        },
        column_order=existing,
    )

    st.markdown("</div>", unsafe_allow_html=True)

    # ---------------- Totals ----------------
    with st.expander("Performance totals", expanded=False):
        entity = st.radio("Entity", ["Horses", "Jockeys", "Trainers"], horizontal=True)
        totals = blank_na(prettify_df(get_totals(df, entity)))
        st.dataframe(totals, use_container_width=True, hide_index=True)

# ============================================================
# RESULTS TAB
# ============================================================
with tab_res:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Official results")

    results = df.copy()

    if "Pos" in results.columns:
        results["sortPos"] = results["Pos"].apply(pos_sort_key)
        results = results.sort_values("sortPos", kind="mergesort")

    res_cols = [
        "Pos",
        "SilkURL",
        "HorseName",
        "SP",
        "PrizeMoney",
        "Post_Jockey",
        "Post_Trainer",
        "RideDescription",
    ]
    res_cols = [c for c in res_cols if c in results.columns]

    results = blank_na(results[res_cols].copy())

    # Detect default currency for this race (used for zeros)
    currency_series = results["PrizeMoney"].astype(str)
    if currency_series.str.contains("€").any():
        default_symbol = "€"
    else:
        default_symbol = "£"

    # Format prize money consistently
    if "PrizeMoney" in results.columns:
        results["PrizeMoney"] = results["PrizeMoney"].apply(
            lambda x: format_prize_money(x, default_symbol)
        )


    # Silk image (hide URL column)
    if "SilkURL" in results.columns:
        results["Silk"] = results["SilkURL"].apply(
            lambda u: f'<img src="{u}" width="26" />' if str(u).strip() else ""
        )
        results = results.drop(columns=["SilkURL"])

    results_display = prettify_df(results)

    # Force exact column order
    desired_order = ["Pos", "Silk", "Horse", "SP", "Prize Money", "Jockey", "Trainer", "Comment"]
    existing = [c for c in desired_order if c in results_display.columns]
    results_display = results_display[existing]

    # Render as HTML so Comment wraps and rows are taller (controlled by CSS)
    st.markdown(results_display.to_html(escape=False, index=False, classes="results-table"), unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)



# ============================================================
# LAST 12 MONTHS TAB
# ============================================================
with tab_12m:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("Last 12 months stats (entities in this race)")

    # ---- Controls ----
    c1, c2 = st.columns([2, 1])

    with c1:
        choice = st.radio("View stats for", ["Horses", "Jockeys", "Trainers"], horizontal=True)

    with c2:
        currency = st.radio("Currency", ["£ GBP", "€ EUR"], horizontal=True)

    target_currency = "EUR" if currency.startswith("€") else "GBP"
    st.caption("ℹ️ Currency conversion assumes £1 = €1.15. Totals may include mixed GBP/EUR earnings.")

    # ---- Entity selection ----
    if choice == "Horses":
        entity_type = "HORSE"
        entity_label = "Horse"
        names = sorted(df["HorseName"].dropna().unique()) if "HorseName" in df.columns else []
    elif choice == "Jockeys":
        entity_type = "JOCKEY"
        entity_label = "Jockey"
        if "Jockey" in df.columns:
            names = sorted(df["Jockey"].dropna().unique())
        elif "Post_Jockey" in df.columns:
            names = sorted(df["Post_Jockey"].dropna().unique())
        else:
            names = []
    else:
        entity_type = "TRAINER"
        entity_label = "Trainer"
        if "Trainer" in df.columns:
            names = sorted(df["Trainer"].dropna().unique())
        elif "Post_Trainer" in df.columns:
            names = sorted(df["Post_Trainer"].dropna().unique())
        else:
            names = []

    if not names:
        st.info("No entities found for this race.")
        st.markdown("</div>", unsafe_allow_html=True)
        st.stop()

    # ---- Fetch stats ----
    stats = get_last12m_entity_stats(
        names=names,
        as_of_date=selected_date,
        entity_type=entity_type
    )

    if stats.empty:
        st.info("No historical stats available.")
        st.markdown("</div>", unsafe_allow_html=True)
        st.stop()

    # ---- Build display columns ----
    stats = stats.rename(columns={"Entity": entity_label})

    for col in ["Races", "Wins", "Places"]:
        stats[col] = pd.to_numeric(stats[col], errors="coerce").fillna(0).astype(int)

    stats["Wins + Places"] = stats["Wins"] + stats["Places"]

    races = stats["Races"].replace(0, pd.NA)
    win_pct = (stats["Wins"] / races * 100).round(1)
    place_pct = (stats["Places"] / races * 100).round(1)
    wp_pct = (stats["Wins + Places"] / races * 100).round(1)

    stats["Wins"] = stats["Wins"].astype(str) + " (" + win_pct.fillna(0).astype(str) + "%)"
    stats["Places"] = stats["Places"].astype(str) + " (" + place_pct.fillna(0).astype(str) + "%)"
    stats["Wins + Places"] = stats["Wins + Places"].astype(str) + " (" + wp_pct.fillna(0).astype(str) + "%)"

    # ---- Currency conversion ----
    stats["Total Prize Money"] = stats["PrizeMoneyTotal"].apply(
        lambda x: convert_currency(x, target_currency)
    )

    stats["Total Prize Money"] = (
        stats["Total Prize Money"]
        .fillna(0)
        .round(0)
        .astype(int)
        .apply(lambda x: f"{'€' if target_currency == 'EUR' else '£'}{x:,}")
    )

    stats = stats.drop(columns=["PrizeMoneyTotal"])

    stats = stats[
        [entity_label, "Races", "Wins", "Places", "Wins + Places", "Total Prize Money"]
    ]

    st.dataframe(stats, use_container_width=True, hide_index=True)

    st.markdown("</div>", unsafe_allow_html=True)