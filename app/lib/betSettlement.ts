export type SavedBetSummary = {
  runnerId?: string;
  betType?: "WIN" | "EW";
  stake?: number;
  takenOdds?: string;
};

export type RaceResultSummary = {
  runner_id: string;
  result: string | null;
  resultPosition: number | null;
  originalOdds: string | null;
  placeTerms: number | null;
  placesPaid: number | null;
  ewPossible: boolean | null;
};

export function fractionalOdds(value?: string): number | null {
  const odds = (value ?? "").trim().toUpperCase();
  if (odds === "EVS" || odds === "EVENS") return 1;
  const fraction = odds.match(/^(\d+(?:\.\d+)?)\s*\/\s*(\d+(?:\.\d+)?)$/);
  if (fraction) {
    const denominator = Number(fraction[2]);
    return denominator > 0 ? Number(fraction[1]) / denominator : null;
  }
  const decimal = Number(odds);
  return odds !== "" && Number.isFinite(decimal) && decimal > 1 ? decimal - 1 : null;
}

export function settleBet(bet: SavedBetSummary, result?: RaceResultSummary) {
  const unitStake = Number(bet.stake) || 0;
  const isEW = bet.betType === "EW";
  const stake = unitStake * (isEW ? 2 : 1);
  if (!result) return { status: "Pending", stake, returns: null, profit: null };
  const position = result.resultPosition == null ? null : Number(result.resultPosition);
  if (!result.result?.trim() && position === null)
    return { status: "Pending", stake, returns: null, profit: null };
  const odds = fractionalOdds(bet.takenOdds);
  if (odds === null)
    return { status: "Unavailable", stake, returns: null, profit: null };
  const won = position === 1;
  const placed = isEW && Boolean(result.ewPossible) && position !== null &&
    position >= 1 && position <= Number(result.placesPaid ?? 0);
  const returns = (won ? unitStake * (1 + odds) : 0) +
    (placed ? unitStake * (1 + odds * Number(result.placeTerms ?? 0)) : 0);
  return { status: won ? "Won" : placed ? "Placed" : "Lost", stake, returns, profit: returns - stake };
}
