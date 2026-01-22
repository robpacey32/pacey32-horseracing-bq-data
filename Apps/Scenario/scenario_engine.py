import numpy as np
import pandas as pd


def _safe_int(x):
    try:
        if pd.isna(x):
            return None
        return int(float(str(x).strip()))
    except Exception:
        return None


def _get_ew_terms(runners: int, is_handicap: bool):
    """
    Returns (place_terms_fraction, places_paid, ew_possible_bool)
    Mirrors common UK EW rules used in your notebook.
    """
    if runners is None:
        return (0.0, 0, False)

    # default: no EW if too few runners
    if runners <= 4:
        return (0.0, 0, False)

    # 5-7 runners: 1/4 odds, 2 places (often)
    if 5 <= runners <= 7:
        return (0.25, 2, True)

    # 8-11 runners: 1/5 odds, 3 places (often)
    if 8 <= runners <= 11:
        return (0.20, 3, True)

    # 12-15 runners: 1/4 (handicap) else 1/5, 3 places
    if 12 <= runners <= 15:
        return (0.25 if is_handicap else 0.20, 3, True)

    # 16+ handicap: 1/4 odds, 4 places; otherwise keep 1/5, 3 places
    if runners >= 16 and is_handicap:
        return (0.25, 4, True)

    return (0.20, 3, True)


def calculate_returns_split(row, stake: float, each_way: bool):
    """
    Returns a dict with:
      Staked, Win_Returns, Place_Returns, Total_Returns, Profit

    Assumes:
      - Odds_dec is fractional-decimal (e.g. 9/2 -> 4.5) NOT full decimal odds.
      - Result_Position is 1 for winner, 2/3/etc for placing.
    """
    result = _safe_int(row.get("Result_Position"))
    runners = _safe_int(row.get("Post_RaceRunners"))
    is_handicap = bool(row.get("HandicappedRace"))
    win_ew = row.get("Odds_dec")

    try:
        win_ew = float(win_ew)
    except Exception:
        win_ew = np.nan

    if result is None or runners is None or pd.isna(win_ew):
        return {
            "Staked": np.nan,
            "Win_Returns": np.nan,
            "Place_Returns": np.nan,
            "Total_Returns": np.nan,
            "Profit": np.nan,
        }

    if not each_way:
        # WIN ONLY
        staked = float(stake)
        win_returns = staked * win_ew + staked if result == 1 else 0.0
        total = win_returns
        return {
            "Staked": staked,
            "Win_Returns": win_returns,
            "Place_Returns": 0.0,
            "Total_Returns": total,
            "Profit": total - staked,
        }

    # EACH WAY
    place_terms, places_paid, ew_possible = _get_ew_terms(runners, is_handicap)

    stake_win = float(stake)
    stake_place = float(stake) if ew_possible else 0.0
    staked = stake_win + stake_place

    # Win part
    win_returns = stake_win * win_ew + stake_win if result == 1 else 0.0

    # Place part
    placed = (result is not None) and (result <= places_paid) and (places_paid > 0)
    place_odds = win_ew * place_terms
    place_returns = stake_place * place_odds + stake_place if placed else 0.0

    total = win_returns + place_returns
    return {
        "Staked": staked,
        "Win_Returns": win_returns,
        "Place_Returns": place_returns,
        "Total_Returns": total,
        "Profit": total - staked,
    }


def apply_strategy(df: pd.DataFrame, stake: float, each_way: bool, stake_mode: str):
    """
    stake_mode:
      - "Per horse": stake is per selected runner
      - "Per race": stake is per race, split equally across selected runners in that race
    """
    out = df.copy()

    if stake_mode == "Per race":
        # split stake across selections within each race
        # assumes RaceDateTime+RaceLocation+RaceTime-ish uniquely identifies a race; if you have a RaceID use that.
        race_key_cols = [c for c in ["RaceDateTime", "RaceLocation", "RaceTime"] if c in out.columns]
        if not race_key_cols:
            # fallback: treat all as one race (still works, just not ideal)
            race_key_cols = ["__all__"]
            out["__all__"] = "all"

        counts = out.groupby(race_key_cols)["HorseName"].transform("count").clip(lower=1)
        per_horse_stake = float(stake) / counts
    else:
        per_horse_stake = float(stake)

    metrics = out.apply(lambda r: calculate_returns_split(r, per_horse_stake, each_way), axis=1, result_type="expand")
    for col in metrics.columns:
        out[col] = metrics[col]

    return out