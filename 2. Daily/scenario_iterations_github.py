import itertools
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = "horseracing-pacey32-github"
SOURCE_VIEW = "horseracing-pacey32-github.horseraceanalytics.Scenario_1_DataPrep_vw"
OUTPUT_TABLE = "horseracing-pacey32-github.horseraceanalytics.ScenarioIterations_GitHub"


def convert_to_lbs(weight_str):
    if pd.isna(weight_str):
        return np.nan
    try:
        stone, lbs = map(int, str(weight_str).split("-"))
        return stone * 14 + lbs
    except Exception:
        return np.nan


def load_source_data(client: bigquery.Client, date_from: pd.Timestamp) -> pd.DataFrame:
    query = f"""
    SELECT
        RaceDateTime,
        Pre_RaceLocation,
        Pre_RaceGoing,
        Pre_RaceRunners,
        Pre_RaceSurface,
        Pre_RaceDistance,
        HorseName,
        Age,
        Weight,
        LastRun,
        RaceHistoryStats,
        PreRaceRank,
        Result_Position,
        Favourite,
        Odds_dec,
        Win_Returns,
        Place_Returns,
        Total_Returns,
        Staked,
        Distance_Furlongs
    FROM `{SOURCE_VIEW}`
    WHERE DATE(RaceDateTime) >= @date_from
      AND Result_Position IS NOT NULL
    """

    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date_from", "DATE", date_from.date())
            ]
        ),
    )
    return job.result().to_dataframe(create_bqstorage_client=False)


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["RaceDateTime"] = pd.to_datetime(df["RaceDateTime"], errors="coerce")
    df["RaceDate"] = df["RaceDateTime"].dt.date

    df["RaceID"] = (
        df["RaceDateTime"].dt.strftime("%Y-%m-%d %H:%M:%S").astype(str)
        + "|"
        + df["Pre_RaceLocation"].astype(str)
    )

    df = df.rename(columns={
        "Pre_RaceGoing": "RaceGoing",
        "Pre_RaceRunners": "RaceRunners",
        "Pre_RaceSurface": "RaceSurface",
        "Pre_RaceDistance": "RaceDistance",
        "Pre_RaceLocation": "RaceLocation",
    })

    df["RaceGoing"] = (
        df["RaceGoing"]
        .astype(str)
        .str.replace(r"\s*\(.*?\)", "", regex=True)
        .str.strip()
    )

    rhs = df["RaceHistoryStats"].fillna("").astype(str).str.upper()

    df["BF"] = rhs.str.contains(r"\bBF\b", regex=True)
    df["CD"] = rhs.str.contains(r"\bCD\b", regex=True)
    df["C"] = rhs.str.contains(r"(^|[^A-Z])C([^A-Z]|$)", regex=True) & ~df["CD"]
    df["D"] = rhs.str.contains(r"(^|[^A-Z])D([^A-Z]|$)", regex=True) & ~df["CD"]

    df["fav"] = df["Favourite"].astype(str).str.lower().eq("f")

    going = df["RaceGoing"].astype(str).str.lower()
    df["Going_Standard"] = going.str.startswith("standard")
    df["Going_GtS"] = going.str.startswith("good to soft")
    df["Going_Soft"] = going.str.startswith("soft")
    df["Going_Good"] = going.eq("good")
    df["Going_Heavy"] = going.str.startswith("heavy")
    df["Going_GtF"] = going.str.startswith("good to firm")

    surface = (
        df["RaceSurface"]
        .astype(str)
        .str.lower()
        .str.replace("-", "", regex=False)
        .str.replace(" ", "", regex=False)
    )
    df["Track_Turf"] = surface.isin(["turf", "grass"])
    df["Track_AW"] = surface.isin(["allweather", "aw", "tapeta", "polytrack", "fibresand"])
    df["Track_Poly"] = surface.eq("polytrack")

    df["Weight_lbs"] = df["Weight"].apply(convert_to_lbs)
    df["LastRun"] = pd.to_numeric(df["LastRun"], errors="coerce")
    df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
    df["PreRaceRank"] = pd.to_numeric(df["PreRaceRank"], errors="coerce")
    df["Odds_dec"] = pd.to_numeric(df["Odds_dec"], errors="coerce")
    df["Staked"] = pd.to_numeric(df["Staked"], errors="coerce")
    df["Total_Returns"] = pd.to_numeric(df["Total_Returns"], errors="coerce")
    df["Result_Position"] = pd.to_numeric(df["Result_Position"], errors="coerce")

    df["Profit"] = df["Total_Returns"].fillna(0) - df["Staked"].fillna(0)

    return df


def build_conditions(df: pd.DataFrame) -> dict:
    return {
        "BF == 1": df["BF"] == 1,
        "Odds_dec < 10": df["Odds_dec"] < 10,
        "PreRaceRank <= 3": df["PreRaceRank"] <= 3,
        "Going_Standard": df["Going_Standard"] == 1,
        "CD == 0": df["CD"] == 0,
        "C == 0": df["C"] == 0,
        "D == 0": df["D"] == 0,
        "LastRun < 30": df["LastRun"] < 30,
        "LastRun >= 30": df["LastRun"] >= 30,
        "fav == 1": df["fav"] == 1,
    }


def run_iterations(df: pd.DataFrame) -> pd.DataFrame:
    conditions = build_conditions(df)
    results = []

    for r in range(1, len(conditions) + 1):
        for combo in itertools.combinations(conditions.items(), r):
            name = " & ".join([k for k, _ in combo])

            mask = pd.Series(True, index=df.index)
            for _, cond in combo:
                mask &= cond

            filtered = df.loc[mask].copy()
            if filtered.empty:
                continue

            race_counts = filtered.groupby("RaceID").size()
            valid_races = race_counts[race_counts == 1].index
            filtered_unique = filtered[filtered["RaceID"].isin(valid_races)].copy()

            if filtered_unique.empty:
                continue

            profit = filtered_unique["Profit"].sum()
            bets = len(filtered_unique)
            roi = profit / bets if bets > 0 else 0.0
            strike_rate = (filtered_unique["Result_Position"] == 1).mean() if bets > 0 else 0.0

            active_days = filtered_unique["RaceDate"].nunique()
            total_days = (filtered_unique["RaceDate"].max() - filtered_unique["RaceDate"].min()).days + 1

            bets_per_day = bets / total_days if total_days > 0 else 0.0
            bets_per_week = bets_per_day * 7
            profit_per_bet = profit / bets if bets > 0 else 0.0
            profit_per_day = profit / total_days if total_days > 0 else 0.0
            pct_days_with_bet = active_days / total_days if total_days > 0 else 0.0

            results.append({
                "Conditions": name,
                "Bets": bets,
                "Profit": profit,
                "ROI": roi,
                "StrikeRate": strike_rate,
                "BetsPerDay": bets_per_day,
                "BetsPerWeek": bets_per_week,
                "ActiveDays": active_days,
                "PctDaysWithBet": pct_days_with_bet,
                "ProfitPerBet": profit_per_bet,
                "ProfitPerDay": profit_per_day,
            })

    results_df = pd.DataFrame(results)
    if results_df.empty:
        return results_df

    results_df = results_df.sort_values(
        by=["ProfitPerDay", "ROI", "Bets"],
        ascending=[False, False, False]
    ).reset_index(drop=True)

    return results_df


def write_results(client: bigquery.Client, results_df: pd.DataFrame):
    run_ts = datetime.now(timezone.utc)

    results_df = results_df.copy()
    results_df["Rank"] = range(1, len(results_df) + 1)
    results_df["RunTimestamp"] = run_ts
    results_df["LookbackMonths"] = 3

    results_df = results_df[
        [
            "RunTimestamp",
            "LookbackMonths",
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
    ]

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(results_df, OUTPUT_TABLE, job_config=job_config)
    job.result()


def main():
    client = bigquery.Client(project=PROJECT_ID)
    date_from = pd.Timestamp.utcnow().normalize() - pd.DateOffset(months=3)

    df = load_source_data(client, date_from)
    df = prepare_features(df)
    results_df = run_iterations(df)

    if results_df.empty:
        print("No scenario results generated.")
        return

    write_results(client, results_df)
    print(f"{len(results_df):,} scenario iterations written to {OUTPUT_TABLE}.")


if __name__ == "__main__":
    main()