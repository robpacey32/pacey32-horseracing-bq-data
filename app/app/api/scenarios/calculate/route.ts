import { NextRequest, NextResponse } from "next/server";
import { unstable_rethrow } from "next/navigation";

import { getBigQueryClient } from "@/lib/bigquery";

type BetType = "win" | "each_way";
type StakeAllocation = "per_horse" | "per_race";
type StakeMode = "total" | "per_leg";

interface ScenarioFilters {
  minOdds?: number;
  maxOdds?: number;
  maxPreRaceRank?: number;
  raceType?: string;
  course?: string;
  handicap?: boolean;
  wonInLast3?: boolean;
  win2InLast6?: boolean;
}

interface ScenarioRequest {
  startDate: string;
  endDate: string;
  stake: number;
  betType: BetType;
  stakeAllocation: StakeAllocation;
  stakeMode?: StakeMode;
  filters?: ScenarioFilters;
}

interface DailyRow {
  date: string;
  bets: number | string;
  winners: number | string;
  staked: number | string;
  returns: number | string;
  profit: number | string;
}

const TABLE =
  "`horseracing-pacey32-github.horseraceanalytics.Scenario_App_Data_vw`";

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

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as ScenarioRequest;

    const {
      startDate,
      endDate,
      stake,
      betType,
      stakeAllocation,
      stakeMode = "total",
      filters = {},
    } = body;

    // --------------------------------------------------
    // Validate request
    // --------------------------------------------------

    if (!isValidDate(startDate) || !isValidDate(endDate)) {
      return NextResponse.json(
        { error: "Invalid start or end date." },
        { status: 400 }
      );
    }

    if (startDate > endDate) {
      return NextResponse.json(
        { error: "Start date must be before end date." },
        { status: 400 }
      );
    }

    if (!isFiniteNumber(stake) || stake <= 0 || stake > 100000) {
      return NextResponse.json(
        { error: "Stake must be between 0 and 100,000." },
        { status: 400 }
      );
    }

    if (betType !== "win" && betType !== "each_way") {
      return NextResponse.json(
        { error: "Invalid bet type." },
        { status: 400 }
      );
    }

    if (
      stakeAllocation !== "per_horse" &&
      stakeAllocation !== "per_race"
    ) {
      return NextResponse.json(
        { error: "Invalid stake allocation." },
        { status: 400 }
      );
    }

    if (stakeMode !== "total" && stakeMode !== "per_leg") {
      return NextResponse.json(
        { error: "Invalid stake mode." },
        { status: 400 }
      );
    }

    // --------------------------------------------------
    // Build parameterised scenario filters
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
      stake,
    };

    const numericFilters: Array<{
      key: keyof ScenarioFilters;
      column: string;
      operator: string;
    }> = [
      {
        key: "minOdds",
        column: "OddsDecimal",
        operator: ">=",
      },
      {
        key: "maxOdds",
        column: "OddsDecimal",
        operator: "<=",
      },
      {
        key: "maxPreRaceRank",
        column: "PreRaceRank",
        operator: "<=",
      },
    ];

    for (const filter of numericFilters) {
      const value = filters[filter.key];

      if (value === undefined) continue;

      if (!isFiniteNumber(value)) {
        return NextResponse.json(
          { error: `Invalid filter: ${filter.key}` },
          { status: 400 }
        );
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
        return NextResponse.json(
          { error: "Invalid race type." },
          { status: 400 }
        );
      }

      conditions.push("Pre_RaceType = @raceType");
      params.raceType = filters.raceType;
    }

    if (filters.course !== undefined) {
      if (
        typeof filters.course !== "string" ||
        filters.course.length > 100
      ) {
        return NextResponse.json(
          { error: "Invalid course." },
          { status: 400 }
        );
      }

      conditions.push(
        "LOWER(Pre_RaceLocation) = LOWER(@course)"
      );

      params.course = filters.course;
    }

    const booleanFilters: Array<{
      key: keyof ScenarioFilters;
      column: string;
    }> = [
      { key: "handicap", column: "HandicappedRace" },
      { key: "wonInLast3", column: "WonInLast3" },
      { key: "win2InLast6", column: "Win2InLast6" },
    ];

    for (const filter of booleanFilters) {
      const value = filters[filter.key];

      if (value === undefined) continue;

      if (typeof value !== "boolean") {
        return NextResponse.json(
          { error: `Invalid filter: ${filter.key}` },
          { status: 400 }
        );
      }

      conditions.push(`${filter.column} = @${filter.key}`);
      params[filter.key] = value;
    }

    // --------------------------------------------------
    // Stake configuration
    // --------------------------------------------------

    // For each-way:
    // total mode: £10 means £5 win + £5 place
    // per_leg mode: £10 means £10 win + £10 place

    const totalStake =
      betType === "each_way" && stakeMode === "per_leg"
        ? stake * 2
        : stake;

    const winLegStake =
      betType === "each_way"
        ? totalStake / 2
        : totalStake;

    const placeLegStake =
      betType === "each_way"
        ? totalStake / 2
        : 0;

    params.winLegStake = winLegStake;
    params.placeLegStake = placeLegStake;

    const allocationDivisor =
      stakeAllocation === "per_race"
        ? "qualifying_horses_in_race"
        : "1";

    // --------------------------------------------------
    // BigQuery calculation
    // --------------------------------------------------

    const query = `
      WITH filtered AS (

        SELECT
          RaceDateDt,
          Pre_RaceLocation,
          Pre_RaceTime,
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
          RaceDateDt,

          Result_Position = 1 AS is_winner,

          win_stake
            + IF(@placeLegStake > 0 AND ew_possible, place_stake, 0)
            AS amount_staked,

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

      )

      SELECT
        CAST(RaceDateDt AS STRING) AS date,

        COUNT(*) AS bets,

        COUNTIF(is_winner) AS winners,

        SUM(amount_staked) AS staked,

        SUM(amount_returned) AS returns,

        SUM(amount_returned - amount_staked) AS profit

      FROM settled

      GROUP BY RaceDateDt
      ORDER BY RaceDateDt
    `;

    const bigquery = getBigQueryClient();

    const [rows] = await bigquery.query({
      query,
      params,
      maximumBytesBilled: "10000000000",
    });

    // --------------------------------------------------
    // Build daily performance
    // --------------------------------------------------

    let cumulativeProfit = 0;
    let cumulativeStaked = 0;

    let peakProfit = 0;
    let maxDrawdown = 0;

    let totalBets = 0;
    let totalWinners = 0;
    let totalStaked = 0;
    let totalReturns = 0;

    const dailyPerformance = (rows as DailyRow[]).map((row) => {
      const profit = Number(row.profit);
      const staked = Number(row.staked);
      const returns = Number(row.returns);

      totalBets += Number(row.bets);
      totalWinners += Number(row.winners);
      totalStaked += staked;
      totalReturns += returns;

      cumulativeProfit += profit;
      cumulativeStaked += staked;

      peakProfit = Math.max(peakProfit, cumulativeProfit);

      maxDrawdown = Math.max(
        maxDrawdown,
        peakProfit - cumulativeProfit
      );

      return {
        date: row.date,
        bets: Number(row.bets),
        winners: Number(row.winners),
        staked: round(staked),
        returns: round(returns),
        profit: round(profit),
        cumulativeProfit: round(cumulativeProfit),
        cumulativeROI:
          cumulativeStaked > 0
            ? round(
                (cumulativeProfit / cumulativeStaked) * 100
              )
            : 0,
      };
    });

    const profit = totalReturns - totalStaked;

    // --------------------------------------------------
    // Response
    // --------------------------------------------------

    return NextResponse.json({
      summary: {
        bets: totalBets,
        winners: totalWinners,

        strikeRate:
          totalBets > 0
            ? round((totalWinners / totalBets) * 100)
            : 0,

        staked: round(totalStaked),
        returns: round(totalReturns),
        profit: round(profit),

        roi:
          totalStaked > 0
            ? round((profit / totalStaked) * 100)
            : 0,

        maxDrawdown: round(maxDrawdown),
      },

      dailyPerformance,

      settings: {
        startDate,
        endDate,
        stake,
        betType,
        stakeAllocation,
        stakeMode,
        filters,
      },
    });

  } catch (error) {
    unstable_rethrow(error);

    console.error("Scenario calculation error:", error);

    return NextResponse.json(
      { error: "Failed to calculate scenario." },
      { status: 500 }
    );
  }
}