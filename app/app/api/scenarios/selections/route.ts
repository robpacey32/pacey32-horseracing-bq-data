import { NextRequest, NextResponse } from "next/server";
import { unstable_rethrow } from "next/navigation";

import { getBigQueryClient } from "@/lib/bigquery";

// --------------------------------------------------
// Types
// --------------------------------------------------

type BetType = "win" | "each_way";
type StakeAllocation = "per_horse" | "per_race";
type StakeMode = "total" | "per_leg";
type ResultFilter = "all" | "winners" | "losers";

interface ScenarioFilters {
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

interface SelectionsRequest {
  startDate: string;
  endDate: string;
  stake: number;
  betType: BetType;
  stakeAllocation: StakeAllocation;
  stakeMode?: StakeMode;
  filters?: ScenarioFilters;

  page?: number;
  pageSize?: number;
  resultFilter?: ResultFilter;
}

interface SelectionRow {
  date: string;
  course: string | null;
  raceTime: string | null;
  raceName: string | null;
  horseName: string | null;
  horseNumber: string | null;
  odds: number | string;
  position: number | string;
  isWinner: boolean;
  isEachWayEligible: boolean | null;
  placesPaid: number | string | null;
  placeTerms: number | string | null;
  staked: number | string;
  returns: number | string;
  profit: number | string;
}

interface TotalsRow {
  totalSelections: number | string;
  winners: number | string;
  staked: number | string | null;
  returns: number | string | null;
  profit: number | string | null;
}

const TABLE =
  "`horseracing-pacey32-github.horseraceanalytics.Scenario_App_Data_vw`";

// --------------------------------------------------
// Helpers
// --------------------------------------------------

function isValidDate(value: unknown): value is string {
  if (typeof value !== "string") return false;

  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }

  const parsed = new Date(`${value}T00:00:00Z`);

  return (
    !Number.isNaN(parsed.getTime()) &&
    parsed.toISOString().slice(0, 10) === value
  );
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function round(value: number, decimals = 2): number {
  const factor = 10 ** decimals;
  return Math.round((value + Number.EPSILON) * factor) / factor;
}

function badRequest(message: string) {
  return NextResponse.json(
    { error: message },
    { status: 400 }
  );
}

// --------------------------------------------------
// POST: Historical selections
// --------------------------------------------------

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as SelectionsRequest;

    if (!body || typeof body !== "object" || Array.isArray(body)) {
      return badRequest("Invalid request body.");
    }

    const {
      startDate,
      endDate,
      stake,
      betType,
      stakeAllocation,
      stakeMode = "total",
      filters = {},
      page = 1,
      pageSize = 50,
      resultFilter = "all",
    } = body;

    // --------------------------------------------------
    // 1. Validate core settings
    // --------------------------------------------------

    if (!isValidDate(startDate) || !isValidDate(endDate)) {
      return badRequest("Invalid start or end date.");
    }

    if (startDate > endDate) {
      return badRequest("Start date must be before end date.");
    }

    if (
      !isFiniteNumber(stake) ||
      stake <= 0 ||
      stake > 100000
    ) {
      return badRequest("Stake must be between 0 and 100,000.");
    }

    if (betType !== "win" && betType !== "each_way") {
      return badRequest("Invalid bet type.");
    }

    if (
      stakeAllocation !== "per_horse" &&
      stakeAllocation !== "per_race"
    ) {
      return badRequest("Invalid stake allocation.");
    }

    if (stakeMode !== "total" && stakeMode !== "per_leg") {
      return badRequest("Invalid stake mode.");
    }

    if (
      !Number.isInteger(page) ||
      page < 1 ||
      page > 100000
    ) {
      return badRequest("Page must be a positive integer.");
    }

    if (
      !Number.isInteger(pageSize) ||
      pageSize < 1 ||
      pageSize > 100
    ) {
      return badRequest("Page size must be between 1 and 100.");
    }

    if (!["all", "winners", "losers"].includes(resultFilter)) {
      return badRequest("Invalid result filter.");
    }

    if (
      !filters ||
      typeof filters !== "object" ||
      Array.isArray(filters)
    ) {
      return badRequest("Invalid filters.");
    }

    // --------------------------------------------------
    // 2. Build filters
    // --------------------------------------------------

    const conditions: string[] = [
      "RaceDateDt BETWEEN DATE(@startDate) AND DATE(@endDate)",
      "OddsDecimal IS NOT NULL",
      "OddsDecimal >= 1",
      "Result_Position IS NOT NULL",
    ];

    const params: Record<string, string | number | boolean> = {
      startDate,
      endDate,
    };

    const numericFilters: Array<{
      key:
        | "minOdds"
        | "maxOdds"
        | "minPreRaceRank"
        | "maxPreRaceRank"
        | "minDistance"
        | "maxDistance";
      column: string;
      operator: ">=" | "<=";
    }> = [
      { key: "minOdds", column: "OddsDecimal", operator: ">=" },
      { key: "maxOdds", column: "OddsDecimal", operator: "<=" },
      {
        key: "minPreRaceRank",
        column: "PreRaceRank",
        operator: ">=",
      },
      {
        key: "maxPreRaceRank",
        column: "PreRaceRank",
        operator: "<=",
      },
      {
        key: "minDistance",
        column: "Distance_Furlongs",
        operator: ">=",
      },
      {
        key: "maxDistance",
        column: "Distance_Furlongs",
        operator: "<=",
      },
    ];

    for (const filter of numericFilters) {
      const value = filters[filter.key];

      if (value === undefined) continue;

      if (!isFiniteNumber(value)) {
        return badRequest(`Invalid filter: ${filter.key}`);
      }

      conditions.push(
        `${filter.column} ${filter.operator} @${filter.key}`
      );

      params[filter.key] = value;
    }

    if (filters.raceType !== undefined) {
      if (
        !["Flat", "Jumps", "Unknown"].includes(filters.raceType)
      ) {
        return badRequest("Invalid race type.");
      }

      conditions.push("Pre_RaceType = @raceType");
      params.raceType = filters.raceType;
    }

    if (filters.course !== undefined) {
      if (
        typeof filters.course !== "string" ||
        filters.course.trim().length === 0 ||
        filters.course.length > 100
      ) {
        return badRequest("Invalid course.");
      }

      conditions.push(
        "LOWER(Pre_RaceLocation) = LOWER(@course)"
      );

      params.course = filters.course.trim();
    }

    const textFilters: Array<{
      key: "raceClass" | "going" | "surface";
      column: string;
    }> = [
      { key: "raceClass", column: "Pre_RaceClass" },
      { key: "going", column: "Pre_RaceGoing" },
      { key: "surface", column: "Pre_RaceSurface" },
    ];

    for (const filter of textFilters) {
      const value = filters[filter.key];

      if (value === undefined) continue;

      if (
        typeof value !== "string" ||
        value.trim().length === 0 ||
        value.length > 100
      ) {
        return badRequest(`Invalid filter: ${filter.key}`);
      }

      conditions.push(
        `LOWER(${filter.column}) = LOWER(@${filter.key})`
      );

      params[filter.key] = value.trim();
    }

    const booleanFilters: Array<{
      key:
        | "handicap"
        | "wonInLast3"
        | "win2InLast6"
        | "courseWinner"
        | "distanceWinner"
        | "courseDistanceWinner"
        | "eachWayEligible";
      column: string;
    }> = [
      { key: "handicap", column: "HandicappedRace" },
      { key: "wonInLast3", column: "WonInLast3" },
      { key: "win2InLast6", column: "Win2InLast6" },
      { key: "courseWinner", column: "RHS_C" },
      { key: "distanceWinner", column: "RHS_D" },
      { key: "courseDistanceWinner", column: "RHS_CD" },
      { key: "eachWayEligible", column: "ew_possible" },
    ];

    for (const filter of booleanFilters) {
      const value = filters[filter.key];

      if (value === undefined) continue;

      if (typeof value !== "boolean") {
        return badRequest(`Invalid filter: ${filter.key}`);
      }

      conditions.push(`${filter.column} = @${filter.key}`);
      params[filter.key] = value;
    }

    if (filters.favouriteStatus !== undefined) {
      if (
        filters.favouriteStatus !== "favourite" &&
        filters.favouriteStatus !== "not_favourite"
      ) {
        return badRequest("Invalid favourite status.");
      }

      conditions.push(
        filters.favouriteStatus === "favourite"
          ? "PreRaceRank = 1"
          : "PreRaceRank > 1"
      );
    }

    // --------------------------------------------------
    // 3. Stake settings
    // --------------------------------------------------

    const totalStake =
      betType === "each_way" && stakeMode === "per_leg"
        ? stake * 2
        : stake;

    params.winLegStake =
      betType === "each_way"
        ? totalStake / 2
        : totalStake;

    params.placeLegStake =
      betType === "each_way"
        ? totalStake / 2
        : 0;

    const allocationDivisor =
      stakeAllocation === "per_race"
        ? "qualifying_horses_in_race"
        : "1";

    // --------------------------------------------------
    // 4. Pagination and result filtering
    // --------------------------------------------------

    const offset = (page - 1) * pageSize;

    params.pageSize = pageSize;
    params.offset = offset;

    const resultCondition =
      resultFilter === "winners"
        ? "is_winner"
        : resultFilter === "losers"
          ? "NOT is_winner"
          : "TRUE";

    // --------------------------------------------------
    // 5. BigQuery query
    // --------------------------------------------------

    const query = `
      WITH filtered AS (

        SELECT
          RaceDateDt,
          Pre_RaceLocation,
          Pre_RaceTime,
          Pre_RaceName,
          HorseName,
          HorseNumber,
          OddsDecimal,
          Result_Position,
          places_paid,
          place_terms,
          ew_possible

        FROM ${TABLE}

        WHERE ${conditions.join("\n          AND ")}

      ),

      selections AS (

        SELECT
          *,

          COUNT(*) OVER (
            PARTITION BY
              RaceDateDt,
              Pre_RaceLocation,
              Pre_RaceTime
          ) AS qualifying_horses_in_race

        FROM filtered

      ),

      stakes AS (

        SELECT
          *,

          SAFE_DIVIDE(
            @winLegStake,
            ${allocationDivisor}
          ) AS win_stake,

          SAFE_DIVIDE(
            @placeLegStake,
            ${allocationDivisor}
          ) AS place_stake

        FROM selections

      ),

      settled AS (

        SELECT
          CAST(RaceDateDt AS STRING) AS date,
          Pre_RaceLocation AS course,
          CAST(Pre_RaceTime AS STRING) AS raceTime,
          Pre_RaceName AS raceName,
          HorseName AS horseName,
          HorseNumber AS horseNumber,
          OddsDecimal AS odds,
          Result_Position AS position,

          Result_Position = 1 AS is_winner,
          ew_possible AS isEachWayEligible,
          places_paid AS placesPaid,
          place_terms AS placeTerms,

          win_stake
            + IF(
                @placeLegStake > 0 AND ew_possible,
                place_stake,
                0
              ) AS amount_staked,

          IF(
            Result_Position = 1,
            win_stake * OddsDecimal,
            0
          )
          +
          IF(
            @placeLegStake > 0
            AND ew_possible
            AND Result_Position BETWEEN 1 AND places_paid,

            place_stake * (
              1 + (OddsDecimal - 1) * place_terms
            ),

            0
          ) AS amount_returned

        FROM stakes

      ),

      final_selections AS (

        SELECT
          *,
          amount_returned - amount_staked AS profit
        FROM settled

      ),

      totals AS (

        SELECT
          COUNT(*) AS totalSelections,
          COUNTIF(is_winner) AS winners,
          COALESCE(SUM(amount_staked), 0) AS staked,
          COALESCE(SUM(amount_returned), 0) AS returns,
          COALESCE(SUM(profit), 0) AS profit
        FROM final_selections

      ),

      paged AS (

        SELECT *
        FROM final_selections
        WHERE ${resultCondition}

        ORDER BY
          date DESC,
          raceTime DESC,
          course,
          horseName

        LIMIT @pageSize
        OFFSET @offset

      )

      SELECT
        (
          SELECT AS STRUCT
            totalSelections,
            winners,
            staked,
            returns,
            profit
          FROM totals
        ) AS totals,

        (
          SELECT COUNT(*)
          FROM final_selections
          WHERE ${resultCondition}
        ) AS filteredCount,

        ARRAY(
          SELECT AS STRUCT
            date,
            course,
            raceTime,
            raceName,
            horseName,
            horseNumber,
            odds,
            position,
            is_winner AS isWinner,
            isEachWayEligible,
            placesPaid,
            placeTerms,
            amount_staked AS staked,
            amount_returned AS returns,
            profit
          FROM paged
        ) AS selections
    `;

    // --------------------------------------------------
    // 6. Execute
    // --------------------------------------------------

    const bigquery = getBigQueryClient();

    const [rows] = await bigquery.query({
      query,
      params,
      maximumBytesBilled: "10000000000",
    });

    const row = rows[0] as
      | {
          totals: TotalsRow;
          filteredCount: number | string;
          selections: SelectionRow[];
        }
      | undefined;

    if (!row) {
      throw new Error("BigQuery returned no selections response.");
    }

    // --------------------------------------------------
    // 7. Format response
    // --------------------------------------------------

    const selections = row.selections.map((selection) => ({
      date: selection.date,
      course: selection.course,
      raceTime: selection.raceTime,
      raceName: selection.raceName,
      horseName: selection.horseName,
      horseNumber: selection.horseNumber,

      odds: Number(selection.odds),
      position: Number(selection.position),
      isWinner: selection.isWinner,
      isEachWayEligible: selection.isEachWayEligible,

      placesPaid:
        selection.placesPaid === null
          ? null
          : Number(selection.placesPaid),

      placeTerms:
        selection.placeTerms === null
          ? null
          : Number(selection.placeTerms),

      staked: round(Number(selection.staked)),
      returns: round(Number(selection.returns)),
      profit: round(Number(selection.profit)),
    }));

    const totalSelections = Number(
      row.totals.totalSelections
    );

    const filteredCount = Number(row.filteredCount);

    return NextResponse.json({
      selections,

      pagination: {
        page,
        pageSize,
        totalItems: filteredCount,
        totalPages: Math.ceil(filteredCount / pageSize),
      },

      totals: {
        selections: totalSelections,
        winners: Number(row.totals.winners),
        staked: round(Number(row.totals.staked)),
        returns: round(Number(row.totals.returns)),
        profit: round(Number(row.totals.profit)),
      },

      settings: {
        startDate,
        endDate,
        stake,
        betType,
        stakeAllocation,
        stakeMode,
        filters,
        resultFilter,
      },
    });
  } catch (error) {
    unstable_rethrow(error);

    console.error("Scenario selections error:", error);

    return NextResponse.json(
      { error: "Failed to retrieve scenario selections." },
      { status: 500 }
    );
  }
}