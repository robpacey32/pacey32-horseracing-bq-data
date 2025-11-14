import streamlit as st
import pandas as pd
from pathlib import Path

from data.bigquery_functions import (
    get_races_for_date,
    get_single_race,
    get_totals,
)


# ------------------------
# CSS injection
# ------------------------
def inject_css():
    css_path = Path("static/styles.css")
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)


# ------------------------
# Sort positions helper
# ------------------------
def pos_sort_key(val):
    if pd.isna(val): return 999
    s = str(val).lower().strip()
    for suf in ["st", "nd", "rd", "th"]:
        if s.endswith(suf):
            s = s[:-len(suf)]
    if s.isdigit():
        return int(s)
    return 900


# ------------------------
# App Config
# ------------------------
st.set_page_config(
    page_title="Pacey32 Race Viewer",
    page_icon="🏇",
    layout="wide",
)

inject_css()

st.title("Pacey32 Race Viewer")

# ------------------------
# STEP 1: Calendar Picker
# ------------------------
selected_date = st.date_input("Select Race Date")

if not selected_date:
    st.info("Please select a date to continue.")
    st.stop()

# ------------------------
# STEP 2: Load races on this date
# ------------------------
races_for_day = get_races_for_date(selected_date)

if races_for_day.empty:
    st.warning("No races found on this date.")
    st.stop()

# ------------------------
# STEP 3: Location selector
# ------------------------
loc_options = sorted(races_for_day["Pre_RaceLocation"].unique())
selected_location = st.selectbox("Race Location", loc_options)

# ------------------------
# STEP 4: Time selector
# ------------------------
time_options = (
    races_for_day[races_for_day["Pre_RaceLocation"] == selected_location]["Pre_RaceTime"]
    .dropna()
    .unique()
)
selected_time = st.selectbox("Race Time", sorted(time_options))

# ------------------------
# STEP 5: Identify selected race key
# ------------------------
race_key = races_for_day[
    (races_for_day["Pre_RaceLocation"] == selected_location)
    & (races_for_day["Pre_RaceTime"] == selected_time)
]["Pre_SourceURL"].iloc[0]

# ------------------------
# STEP 6: Load that single race
# ------------------------
df = get_single_race(race_key)

if df.empty:
    st.error("No race data returned from BigQuery.")
    st.stop()

st.markdown("---")
tab_pre, tab_res, tab_tot = st.tabs(["Pre-Race", "Results", "Totals"])

# ==================================================================
# TAB 1 — PRE-RACE
# ==================================================================
with tab_pre:
    st.subheader("Race Info")

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

    st.dataframe(df[info_cols].drop_duplicates(), use_container_width=True, hide_index=True)

    st.subheader("Declared Runners (Pre-Race)")

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
        "Odds",
        "Jockey",
        "Trainer",
    ]
    prerace_cols = [c for c in prerace_cols if c in df.columns]

    prerace = df[prerace_cols].copy()

    if "SilkURL" in prerace.columns:
        prerace["Silk"] = prerace["SilkURL"].apply(
            lambda u: f'<img src="{u}" width="26" />' if pd.notna(u) else ""
        )
        ordered = ["HorseNumber", "StallNumber", "Silk", "HorseName"]
        for c in prerace.columns:
            if c not in ordered and c not in ["SilkURL"]:
                ordered.append(c)
        st.write(prerace[ordered].to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.dataframe(prerace, use_container_width=True, hide_index=True)


# ==================================================================
# TAB 2 — RESULTS
# ==================================================================
with tab_res:
    st.subheader("Official Race Results")

    results = df.copy()

    # Sort by position
    if "Pos" in results.columns:
        results["sortPos"] = results["Pos"].apply(pos_sort_key)
        results = results.sort_values("sortPos")

    res_cols = [
        "Pos",
        "SilkURL",
        "HorseName",
        "Result",
        "SP",
        "PrizeMoney",
        "Post_Jockey",
        "Post_Trainer",
        "RideDescription",
    ]
    res_cols = [c for c in res_cols if c in results.columns]

    if "SilkURL" in res_cols:
        results["Silk"] = results["SilkURL"].apply(
            lambda u: f'<img src="{u}" width="26" />' if pd.notna(u) else ""
        )
        ordered = ["Pos", "Silk", "HorseName"] + [c for c in res_cols if c not in ["SilkURL", "Pos", "HorseName"]]
        st.write(results[ordered].to_html(escape=False, index=False), unsafe_allow_html=True)
    else:
        st.dataframe(results[res_cols], use_container_width=True, hide_index=True)


# ==================================================================
# TAB 3 — TOTALS
# ==================================================================
with tab_tot:
    st.subheader("Performance Totals")

    entity = st.radio("Select entity:", ["Horses", "Jockeys", "Trainers"], horizontal=True)

    totals = get_totals(df, entity)

    st.dataframe(totals, use_container_width=True, hide_index=True)