import streamlit as st
import pandas as pd
from pathlib import Path

from data.bigquery_functions import load_race_data


# ---------- Helpers ----------

def inject_css():
    css_path = Path("static/styles.css")
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)


def sort_position(series: pd.Series) -> pd.Series:
    """Convert Pos values like '1', '1st', 'PU', None into a numeric sort key."""
    def to_key(x):
        if pd.isna(x):
            return 999
        s = str(x).strip().lower()
        # Strip ordinal suffixes
        for suf in ["st", "nd", "rd", "th"]:
            if s.endswith(suf):
                s = s[:-len(suf)]
                break
        # Non-numeric like 'PU', 'F', etc go to bottom
        if not s.isdigit():
            return 900
        return int(s)
    return series.map(to_key)


def flag_non_runner(row) -> bool:
    """
    Best-effort non-runner detection based on Result / Pos fields.
    Adjust this if your pipeline later gets an explicit NR flag.
    """
    res = str(row.get("Result", "") or "").upper()
    pos = str(row.get("Pos", "") or "").upper()

    # Common NR markers
    if "NR" in res or "NON RUNNER" in res:
        return True
    if pos in ["NR", "NON RUNNER"]:
        return True

    # If absolutely no result / position / post data, treat as NR
    if not res and not pos and pd.isna(row.get("Post_HorseNumber")):
        return True

    return False


def compute_totals(df: pd.DataFrame, entity: str) -> pd.DataFrame:
    """
    Compute totals for Horses / Jockeys / Trainers using Post_* fields
    where available, with fallbacks to pre fields.
    """
    work = df.copy()

    # Exclude abandoned / void races if RaceStatus is present
    if "RaceStatus" in work.columns:
        work = work[~work["RaceStatus"].isin(["Abandoned", "Void"])]

    # Derive win/place flags from Pos
    def is_win(pos):
        if pd.isna(pos):
            return False
        s = str(pos).strip().lower()
        for suf in ["st", "nd", "rd", "th"]:
            if s.endswith(suf):
                s = s[:-len(suf)]
                break
        return s.isdigit() and int(s) == 1

    def is_place(pos):
        if pd.isna(pos):
            return False
        s = str(pos).strip().lower()
        for suf in ["st", "nd", "rd", "th"]:
            if s.endswith(suf):
                s = s[:-len(suf)]
                break
        return s.isdigit() and int(s) <= 3

    work["IsWin"] = work["Pos"].apply(is_win) if "Pos" in work.columns else False
    work["IsPlace"] = work["Pos"].apply(is_place) if "Pos" in work.columns else False

    if "PrizeMoney" not in work.columns:
        work["PrizeMoney"] = 0.0

    # Choose group column based on entity type
    if entity == "Horses":
        # Prefer Post_HorseName when present, fallback to HorseName
        if "Post_HorseName" in work.columns:
            work["EntityName"] = work["Post_HorseName"].fillna(work.get("HorseName"))
        else:
            work["EntityName"] = work.get("HorseName")
    elif entity == "Jockeys":
        if "Post_Jockey" in work.columns:
            work["EntityName"] = work["Post_Jockey"].fillna(work.get("Jockey"))
        else:
            work["EntityName"] = work.get("Jockey")
    else:  # Trainers
        if "Post_Trainer" in work.columns:
            work["EntityName"] = work["Post_Trainer"].fillna(work.get("Trainer"))
        else:
            work["EntityName"] = work.get("Trainer")

    work = work[~work["EntityName"].isna()]

    # Race identifier: Pre_SourceURL is your unique race key
    race_id_col = "Pre_SourceURL" if "Pre_SourceURL" in work.columns else "Post_SourceURL"

    summary = (
        work.groupby("EntityName")
        .agg(
            Races=(race_id_col, "nunique"),
            Wins=("IsWin", "sum"),
            Places=("IsPlace", "sum"),
            TotalPrizeMoney=("PrizeMoney", "sum"),
        )
        .reset_index()
    )

    summary["WinPct"] = (summary["Wins"] / summary["Races"]).round(3)
    return summary


# ---------- Streamlit App ----------

st.set_page_config(
    page_title="Pacey32 Race Viewer",
    page_icon="🏇",
    layout="wide",
)

inject_css()

st.title("Pacey32 Race Viewer")

# Load all data (RaceFull_Latest joined to RaceSpine_Latest)
master_df = load_race_data()

if master_df.empty:
    st.error("No data returned from BigQuery. Check your view or credentials.")
    st.stop()

# ---------- Global Race Filters (using your actual schema) ----------

# Date selector
if "Pre_RaceDate" not in master_df.columns:
    st.error("Expected column 'Pre_RaceDate' not found in data.")
    st.stop()

dates = sorted(master_df["Pre_RaceDate"].dropna().unique())
selected_date = st.selectbox("Race Date", dates, index=len(dates) - 1 if dates else 0)

# Location selector (by date)
if "Pre_RaceLocation" not in master_df.columns:
    st.error("Expected column 'Pre_RaceLocation' not found in data.")
    st.stop()

location_options = (
    master_df[master_df["Pre_RaceDate"] == selected_date]["Pre_RaceLocation"]
    .dropna()
    .sort_values()
    .unique()
)
selected_location = st.selectbox("Location", location_options)

# Race selector (by date + location)
if "Pre_RaceName" not in master_df.columns:
    st.error("Expected column 'Pre_RaceName' not found in data.")
    st.stop()

race_options = (
    master_df[
        (master_df["Pre_RaceDate"] == selected_date)
        & (master_df["Pre_RaceLocation"] == selected_location)
    ]["Pre_RaceName"]
    .dropna()
    .sort_values()
    .unique()
)
selected_race = st.selectbox("Race", race_options)

# Filtered subset for the chosen race
filtered_df = master_df[
    (master_df["Pre_RaceDate"] == selected_date)
    & (master_df["Pre_RaceLocation"] == selected_location)
    & (master_df["Pre_RaceName"] == selected_race)
].copy()

st.markdown("---")

tab_pre, tab_res, tab_tot = st.tabs(
    ["Pre-Race Line-Up", "Results & Non-Runners", "Totals"]
)

# ---------- TAB 1: Pre-Race ----------

with tab_pre:
    if filtered_df.empty:
        st.warning("No rows found for this race selection.")
    else:
        st.subheader("Race Info")

        race_info_cols = [
            "Pre_RaceClass",
            "Pre_RaceSurface",
            "Pre_RaceDayOfWeek",
            "Pre_RaceGoing",
            "Pre_RaceDistance",
            "Pre_RaceRunners",
            "Pre_RaceTime",
            "RaceStatus",
            "AbandonmentReason",
        ]
        race_info_cols = [c for c in race_info_cols if c in filtered_df.columns]

        race_info = filtered_df[race_info_cols].drop_duplicates() if race_info_cols else pd.DataFrame()
        st.dataframe(race_info, use_container_width=True, hide_index=True)

        st.markdown("### Pre-Race Line-Up")

        # Columns available from your schema for pre-race
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
            "Commentary",
        ]
        prerace_cols = [c for c in prerace_cols if c in filtered_df.columns]

        prerace_view = filtered_df[prerace_cols].copy()

        # Convert SilkURL to image column if present
        if "SilkURL" in prerace_view.columns:
            prerace_view["Silk"] = prerace_view["SilkURL"].apply(
                lambda u: f'<img src="{u}" width="26" />' if pd.notna(u) else ""
            )
            columns_order = (
                ["HorseNumber", "StallNumber", "Silk", "HorseName"]
                + [c for c in prerace_view.columns
                   if c not in ["HorseNumber", "StallNumber", "Silk", "SilkURL", "HorseName"]]
            )
            columns_order = [c for c in columns_order if c in prerace_view.columns]
            st.write(
                prerace_view[columns_order].to_html(escape=False, index=False),
                unsafe_allow_html=True,
            )
        else:
            st.dataframe(prerace_view, use_container_width=True, hide_index=True)

        st.caption("Line-up at declaration time (before non-runners are removed).")

# ---------- TAB 2: Results & Non-Runners ----------

with tab_res:
    if filtered_df.empty:
        st.warning("No rows found for this race selection.")
    else:
        st.subheader("Race Results")

        results_df = filtered_df.copy()

        # Tag non-runners
        results_df["IsNonRunner"] = results_df.apply(flag_non_runner, axis=1)

        # Runners only
        runners = results_df[~results_df["IsNonRunner"]].copy()

        # Sort by Pos if available
        if "Pos" in runners.columns:
            runners["__sort_pos"] = sort_position(runners["Pos"])
            runners = runners.sort_values("__sort_pos").drop(columns="__sort_pos")

        result_cols = [
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
        result_cols = [c for c in result_cols if c in runners.columns]

        if not runners.empty:
            if "SilkURL" in result_cols:
                runners["Silk"] = runners["SilkURL"].apply(
                    lambda u: f'<img src="{u}" width="26" />' if pd.notna(u) else ""
                )
                ordered = []
                if "Pos" in result_cols:
                    ordered.append("Pos")
                ordered.append("Silk")
                ordered.append("HorseName") if "HorseName" in result_cols else None
                for c in result_cols:
                    if c not in ["SilkURL", "Pos", "HorseName"]:
                        ordered.append(c)
                ordered = [c for c in ordered if c in runners.columns]
                st.write(
                    runners[ordered].to_html(escape=False, index=False),
                    unsafe_allow_html=True,
                )
            else:
                st.dataframe(runners[result_cols], use_container_width=True, hide_index=True)
        else:
            st.info("No finishing positions recorded for this race.")

        st.markdown("### Non-Runners")

        non_runners = results_df[results_df["IsNonRunner"]].copy()

        if non_runners.empty:
            st.info("No non-runners recorded for this race.")
        else:
            nr_cols = [
                "SilkURL",
                "HorseName",
                "Jockey",
                "Trainer",
                "Result",
            ]
            nr_cols = [c for c in nr_cols if c in non_runners.columns]

            if "SilkURL" in nr_cols:
                non_runners["Silk"] = non_runners["SilkURL"].apply(
                    lambda u: f'<img src="{u}" width="26" />' if pd.notna(u) else ""
                )
                ordered = ["Silk", "HorseName"]
                ordered += [c for c in nr_cols if c not in ["SilkURL", "HorseName"]]
                ordered = [c for c in ordered if c in non_runners.columns]
                st.write(
                    non_runners[ordered].to_html(escape=False, index=False),
                    unsafe_allow_html=True,
                )
            else:
                st.dataframe(non_runners[nr_cols], use_container_width=True, hide_index=True)

# ---------- TAB 3: Totals ----------

with tab_tot:
    st.subheader("Performance Totals")

    entity = st.radio(
        "View totals for:",
        options=["Horses", "Jockeys", "Trainers"],
        horizontal=True,
    )

    totals_df = compute_totals(master_df, entity)

    search = st.text_input(f"Filter {entity.lower()} by name (optional)")
    if search:
        totals_df = totals_df[
            totals_df["EntityName"].str.contains(search, case=False, na=False)
        ]

    totals_df = totals_df.sort_values("TotalPrizeMoney", ascending=False)

    st.dataframe(totals_df, use_container_width=True, hide_index=True)
    st.caption("Races = distinct races; Places = top 3 finishes; Win% = Wins / Races.")
