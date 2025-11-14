import streamlit as st
import pandas as pd
from pathlib import Path

from data.bigquery_functions import (
    load_race_data,
    get_filter_options,
    filter_by_selection,
)


def inject_css():
    css_path = Path("static/styles.css")
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)


def prerace_view(filtered: pd.DataFrame):
    st.subheader("Race Info")

    race_cols = [
        "RaceClass",
        "RaceSurface_Pre",
        "RaceDayOfWeek_Pre",
        "RaceGoing_Pre",
        "RaceDistance_Pre",
        "RaceRunners_Pre",
        "RaceStatus",
        "AbandonmentReason",
    ]
    race_cols = [c for c in race_cols if c in filtered.columns]
    race_info = filtered[race_cols].drop_duplicates() if race_cols else pd.DataFrame()
    st.dataframe(race_info, use_container_width=True, hide_index=True)

    st.markdown("### Pre-Race Line-Up")

    prerace_df = filtered.copy()

    # Remove non-runners from Pre-Race table if we have that flag
    if "IsNonRunner" in prerace_df.columns:
        prerace_df = prerace_df[~prerace_df["IsNonRunner"].fillna(False)]

    display_cols = [
        "SilkURL",
        "HorseName",
        "RaceHistoryStats",
        "Headgear",
        "Odds",
        "Jockey_Pre",
        "Trainer_Pre",
        "LastRun",
    ]
    display_cols = [c for c in display_cols if c in prerace_df.columns]

    if "SilkURL" in display_cols:
        prerace_df = prerace_df.copy()
        prerace_df["Silk"] = prerace_df["SilkURL"].apply(
            lambda u: f'<img src="{u}" width="26" />' if pd.notna(u) else ""
        )
        display_cols = ["Silk"] + [c for c in display_cols if c != "SilkURL"]
        st.write(
            prerace_df[display_cols].to_html(escape=False, index=False),
            unsafe_allow_html=True,
        )
    else:
        st.dataframe(prerace_df[display_cols], use_container_width=True, hide_index=True)

    st.caption("Line-up at declaration time (excluding non-runners).")


def results_view(filtered: pd.DataFrame):
    st.subheader("Race Result")

    results_df = filtered.copy()

    # Only include runners
    if "IsNonRunner" in results_df.columns:
        results_df = results_df[~results_df["IsNonRunner"].fillna(False)]

    result_cols = [
        "FinishingPosition",     # integer or '1st', '2nd' etc
        "SilkURL",
        "HorseName",
        "SP",
        "PrizeMoney",
    ]
    result_cols = [c for c in result_cols if c in results_df.columns]

    if "FinishingPosition" in result_cols:
        # Try sort by numeric position
        def pos_sorter(x):
            if pd.isna(x):
                return 999
            if isinstance(x, str) and x.endswith(("st", "nd", "rd", "th")):
                try:
                    return int(x[:-2])
                except Exception:
                    return 999
            try:
                return int(x)
            except Exception:
                return 999

        results_df = results_df.copy()
        results_df["__pos_sort"] = results_df["FinishingPosition"].apply(pos_sorter)
        results_df = results_df.sort_values("__pos_sort").drop(columns="__pos_sort")

    if "SilkURL" in result_cols:
        results_df = results_df.copy()
        results_df["Silk"] = results_df["SilkURL"].apply(
            lambda u: f'<img src="{u}" width="26" />' if pd.notna(u) else ""
        )
        result_cols = [c for c in result_cols if c != "SilkURL"]
        # Put Silk next to position
        order = []
        if "FinishingPosition" in result_cols:
            order.append("FinishingPosition")
        order.append("Silk")
        order += [c for c in result_cols if c not in ("FinishingPosition",)]
        st.write(
            results_df.assign(Silk=results_df["Silk"])[order].to_html(
                escape=False, index=False
            ),
            unsafe_allow_html=True,
        )
    else:
        st.dataframe(results_df[result_cols], use_container_width=True, hide_index=True)

    st.markdown("### Non-Runners")

    # Detect non-runners
    if "IsNonRunner" in filtered.columns:
        nr_df = filtered[filtered["IsNonRunner"].fillna(False)].copy()
    elif "RunnerStatus" in filtered.columns:
        nr_df = filtered[filtered["RunnerStatus"].str.upper().eq("NR")].copy()
    else:
        nr_df = pd.DataFrame()

    if nr_df.empty:
        st.info("No non-runners recorded for this race.")
    else:
        nr_cols = [
            "SilkURL",
            "HorseName",
            "Jockey_Pre",
            "Trainer_Pre",
            "NRReason",
            "NonRunnerReason",
        ]
        nr_cols = [c for c in nr_cols if c in nr_df.columns]

        if "SilkURL" in nr_cols:
            nr_df = nr_df.copy()
            nr_df["Silk"] = nr_df["SilkURL"].apply(
                lambda u: f'<img src="{u}" width="26" />' if pd.notna(u) else ""
            )
            nr_cols = ["Silk"] + [c for c in nr_cols if c != "SilkURL"]
            st.write(
                nr_df[nr_cols].to_html(escape=False, index=False),
                unsafe_allow_html=True,
            )
        else:
            st.dataframe(nr_df[nr_cols], use_container_width=True, hide_index=True)


def totals_view(master_df: pd.DataFrame):
    st.subheader("Performance Totals")

    view_type = st.radio(
        "View totals for:",
        options=["Horses", "Jockeys", "Trainers"],
        horizontal=True,
    )

    df = master_df.copy()

    # Exclude abandoned / void races if we know about them
    if "RaceStatus" in df.columns:
        df = df[~df["RaceStatus"].isin(["Abandoned", "Void"])]

    # Basic win / place flags
    def is_win(pos):
        try:
            if isinstance(pos, str) and pos.endswith(("st", "nd", "rd", "th")):
                pos = pos[:-2]
            return int(pos) == 1
        except Exception:
            return False

    def is_place(pos):
        try:
            if isinstance(pos, str) and pos.endswith(("st", "nd", "rd", "th")):
                pos = pos[:-2]
            return int(pos) <= 3
        except Exception:
            return False

    if "FinishingPosition" in df.columns:
        df["IsWin"] = df["FinishingPosition"].apply(is_win)
        df["IsPlace"] = df["FinishingPosition"].apply(is_place)
    else:
        df["IsWin"] = False
        df["IsPlace"] = False

    if "PrizeMoney" not in df.columns:
        df["PrizeMoney"] = 0.0

    if view_type == "Horses":
        group_col = "HorseName"
    elif view_type == "Jockeys":
        group_col = "Jockey_Pre"
    else:
        group_col = "Trainer_Pre"

    if group_col not in df.columns:
        st.error(f"Column '{group_col}' not found. Adjust code to match schema.")
        return

    group_df = (
        df.groupby(group_col)
        .agg(
            Races=("RaceURL", "nunique")
            if "RaceURL" in df.columns
            else (group_col, "count"),
            Wins=("IsWin", "sum"),
            Places=("IsPlace", "sum"),
            TotalPrizeMoney=("PrizeMoney", "sum"),
        )
        .reset_index()
    )
    group_df["WinPct"] = (group_df["Wins"] / group_df["Races"]).round(3)

    search = st.text_input(f"Filter {view_type.lower()} by name (optional)")
    if search:
        group_df = group_df[group_df[group_col].str.contains(search, case=False, na=False)]

    group_df = group_df.sort_values("TotalPrizeMoney", ascending=False)

    st.dataframe(group_df, use_container_width=True, hide_index=True)
    st.caption("Races = distinct races run; Places = top 3 finishes; Win% = Wins / Races.")


# -------------------- MAIN APP -------------------- #

st.set_page_config(
    page_title="Pacey32 Race Viewer",
    page_icon="🏇",
    layout="wide",
)

inject_css()

st.title("Pacey32 Race Viewer")

# Load all race data once
master_df = load_race_data()

# Global race selectors across tabs
dates, locations, races = get_filter_options(master_df)

sel_col1, sel_col2, sel_col3 = st.columns(3)

with sel_col1:
    selected_date = st.selectbox(
        "Race Date",
        options=dates,
        index=len(dates) - 1 if dates else 0,
    )

with sel_col2:
    # restrict locations by date
    loc_options = (
        master_df[master_df["RaceDate"] == selected_date]["RaceLocation"]
        .dropna()
        .sort_values()
        .unique()
        if "RaceDate" in master_df.columns and "RaceLocation" in master_df.columns
        else locations
    )
    selected_location = st.selectbox("Location", loc_options if len(loc_options) else ["—"])

with sel_col3:
    subset = master_df.copy()
    if "RaceDate" in subset.columns:
        subset = subset[subset["RaceDate"] == selected_date]
    if "RaceLocation" in subset.columns:
        subset = subset[subset["RaceLocation"] == selected_location]

    race_options = (
        subset["RaceName_Pre"].dropna().sort_values().unique()
        if "RaceName_Pre" in subset.columns
        else races
    )
    selected_race = st.selectbox("Race", race_options if len(race_options) else ["—"])

st.markdown("---")

# Apply filters once and share across tabs
filtered_df = filter_by_selection(master_df, selected_date, selected_location, selected_race)

tab_pre, tab_res, tab_tot = st.tabs(["Pre-Race Line-Up", "Results & Non-Runners", "Totals"])

with tab_pre:
    if filtered_df.empty:
        st.warning("No data for this selection.")
    else:
        prerace_view(filtered_df)

with tab_res:
    if filtered_df.empty:
        st.warning("No data for this selection.")
    else:
        results_view(filtered_df)

with tab_tot:
    totals_view(master_df)
