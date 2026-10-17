import type { ScenarioFilters } from "./types";

type Parameter = string | number | boolean | string[];
export function scenarioWhere(filters: ScenarioFilters, startDate: string, endDate: string) {
  const conditions = ["RaceDateDt BETWEEN DATE(@startDate) AND DATE(@endDate)", "OddsDecimal IS NOT NULL", "OddsDecimal >= 1", "Result_Position IS NOT NULL"];
  const params: Record<string, Parameter> = {startDate, endDate};
  const numeric: Record<string, [string,string]> = {
    minOdds:["OddsDecimal",">="], maxOdds:["OddsDecimal","<="],
    minPreRaceRank:["PreRaceRank",">="], maxPreRaceRank:["PreRaceRank","<="],
    minDistance:["Distance_Furlongs",">="], maxDistance:["Distance_Furlongs","<="]
  };
  for (const [key,[column,op]] of Object.entries(numeric)) {
    const value = filters[key as keyof ScenarioFilters];
    if (value === undefined) continue;
    if (typeof value !== "number" || !Number.isFinite(value)) throw new Error(`Invalid filter: ${key}`);
    conditions.push(`${column} ${op} @${key}`); params[key] = value;
  }
  // Legacy single-value fields remain supported for saved scenarios.
  const multi: Array<{
    plural: "raceTypes" | "courses" | "raceClasses" | "goings" | "surfaces";
    singular: "raceType" | "course" | "raceClass" | "going" | "surface";
    column: string;
  }> = [
    { plural: "raceTypes", singular: "raceType", column: "Pre_RaceType" },
    { plural: "courses", singular: "course", column: "Pre_RaceLocation" },
    { plural: "raceClasses", singular: "raceClass", column: "Pre_RaceClass" },
    { plural: "goings", singular: "going", column: "Pre_RaceGoing" },
    { plural: "surfaces", singular: "surface", column: "Pre_RaceSurface" },
  ];
  for (const { plural, singular, column } of multi) {
    const list = filters[plural] ?? (filters[singular] ? [filters[singular]!] : []);
    if (!Array.isArray(list) || list.length > 250 || list.some(
      value => typeof value !== "string" || !value.trim() || value.length > 100
    )) throw new Error(`Invalid filter: ${plural}`);
    if (plural === "raceTypes" && list.some(value => !["Flat", "Jumps", "Unknown"].includes(value))) {
      throw new Error("Invalid race type.");
    }
    if (list.length > 0) {
      conditions.push(`LOWER(TRIM(${column})) IN UNNEST(@${plural})`);
      params[plural] = list.map(value => value.trim().toLowerCase());
    }
  }
  const booleans: Record<string,string> = {
    handicap:"HandicappedRace", courseWinner:"RHS_C", distanceWinner:"RHS_D",
    courseDistanceWinner:"RHS_CD", beatenFavourite:"RHS_BF", wonInLast3:"WonInLast3",
    win2InLast6:"Win2InLast6", eachWayEligible:"ew_possible"
  };
  for (const [key,column] of Object.entries(booleans)) {
    const value = filters[key as keyof ScenarioFilters];
    if (value === undefined) continue;
    if (typeof value !== "boolean") throw new Error(`Invalid filter: ${key}`);
    conditions.push(`${column.startsWith("RHS_") ? `SAFE_CAST(${column} AS BOOL)` : column} = @${key}`); params[key] = value;
  }
  if (filters.favouriteStatus !== undefined) {
    if (!["favourite","not_favourite"].includes(filters.favouriteStatus)) throw new Error("Invalid favourite status.");
    conditions.push(filters.favouriteStatus === "favourite" ? "PreRaceRank = 1" : "PreRaceRank > 1");
  }
  return {conditions,params};
}
