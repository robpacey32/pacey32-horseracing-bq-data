export interface ScenarioFilters {
  minOdds?: number;
  maxOdds?: number;
  minPreRaceRank?: number;
  maxPreRaceRank?: number;
  favouriteStatus?: "favourite" | "not_favourite";

  raceType?: string;
  course?: string;
  raceClass?: string;
  going?: string;
  surface?: string;
  minDistance?: number;
  maxDistance?: number;
  handicap?: boolean;

  courseWinner?: boolean;
  distanceWinner?: boolean;
  courseDistanceWinner?: boolean;

  wonInLast3?: boolean;
  win2InLast6?: boolean;

  eachWayEligible?: boolean;
}

export interface ScenarioDefinition {
  id: string;
  name: string;
  startDate: string;
  endDate: string;
  stake: number;
  betType: "win" | "each_way";
  stakeAllocation: "per_horse" | "per_race";
  stakeMode: "total" | "per_leg";
  filters: ScenarioFilters;
}

export interface DailyPerformance {
  date: string;
  bets: number;
  winners: number;
  staked: number;
  returns: number;
  profit: number;
  cumulativeProfit: number;
  cumulativeROI: number;
}

export interface ScenarioSummary {
  bets: number;
  winners: number;
  strikeRate: number;
  staked: number;
  returns: number;
  profit: number;
  roi: number;
  maxDrawdown: number;
}

export interface ScenarioResult {
  summary: ScenarioSummary;
  dailyPerformance: DailyPerformance[];
}

export interface ComparedScenario {
  definition: ScenarioDefinition;
  result: ScenarioResult;
}