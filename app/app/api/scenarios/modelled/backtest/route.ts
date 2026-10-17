import { NextRequest, NextResponse } from "next/server";
import { unstable_rethrow } from "next/navigation";
import { getBigQueryClient } from "@/lib/bigquery";

const SOURCE =
  "`horseracing-pacey32-github.horseraceanalytics.Scenario_1_DataPrep_vw`";

const HISTORY =
  "`horseracing-pacey32-github.horseraceanalytics.ScenarioTop20_History`";

const CONDITIONS: Record<string, string> = {
  "BF == 1": "BF",
  "Odds_dec < 10": "Odds_dec < 10",
  "PreRaceRank <= 3": "PreRaceRank <= 3",
  Going_Standard: "Going_Standard",
  "CD == 0": "NOT CD",
  "C == 0": "NOT C",
  "D == 0": "NOT D",
  "LastRun < 30": "LastRunDays < 30",
  "LastRun >= 30": "LastRunDays >= 30",
  "fav == 1": "fav",
};

function parseConditions(value: string): string[] {
  const parts = value.split(/\s*&\s*/).map((part) => part.trim());

  if (
    parts.length === 0 ||
    parts.some((part) => !Object.hasOwn(CONDITIONS, part))
  ) {
    throw new Error("Unsupported model conditions.");
  }

  return parts;
}

function timestampString(value: unknown): string {
  if (value instanceof Date) return value.toISOString();

  if (value && typeof value === "object" && "value" in value) {
    return timestampString((value as { value: unknown }).value);
  }

  return String(value ?? "");
}

function dateString(value: unknown): string {
  if (value && typeof value === "object" && "value" in value) {
    return String((value as { value: unknown }).value);
  }

  return String(value ?? "");
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();

    const {
      runTimestamp,
      rank,
      startDate,
      endDate,
    } = body;

    if (
      typeof runTimestamp !== "string" ||
      !Number.isFinite(Date.parse(runTimestamp)) ||
      !Number.isInteger(rank) ||
      rank < 1 ||
      rank > 20 ||
      typeof startDate !== "string" ||
      typeof endDate !== "string" ||
      !/^\d{4}-\d{2}-\d{2}$/.test(startDate) ||
      !/^\d{4}-\d{2}-\d{2}$/.test(endDate) ||
      !Number.isFinite(Date.parse(startDate)) ||
      !Number.isFinite(Date.parse(endDate)) ||
      startDate > endDate
    ) {
      return NextResponse.json(
        { error: "Invalid backtest parameters." },
        { status: 400 }
      );
    }

    const client = getBigQueryClient();

    // Retrieve the strategy from BigQuery rather than trusting
    // conditions supplied by the browser.
    const [strategies] = await client.query({
      query: `
        SELECT RunTimestamp, Rank, Conditions
        FROM ${HISTORY}
        WHERE TIMESTAMP_TRUNC(RunTimestamp, SECOND)
              = TIMESTAMP_TRUNC(@runTimestamp, SECOND)
          AND Rank = @rank
        LIMIT 1
      `,
      params: {
        runTimestamp: new Date(runTimestamp),
        rank,
      },
      maximumBytesBilled: "1000000000",
    });

    if (!strategies.length) {
      return NextResponse.json(
        { error: "Modelled strategy not found." },
        { status: 404 }
      );
    }

    const strategy = strategies[0];
    const conditions = parseConditions(String(strategy.Conditions));

    const whereConditions = conditions
      .map((condition) => CONDITIONS[condition])
      .join("\nAND ");

    const query = `
      WITH source AS (
        SELECT
          RaceDateDt,
          RaceDateTime,
          Pre_RaceLocation,
          HorseName,
          Result_Position,
          Odds_dec,
          PreRaceRank,
          Favourite,
          Pre_RaceGoing,
          RaceHistoryStats,
          LastRun,
          Staked,
          Total_Returns
        FROM ${SOURCE}
        WHERE RaceDateDt BETWEEN DATE(@startDate)
                             AND DATE(@endDate)
          AND Result_Position IS NOT NULL
      ),

      features AS (
        SELECT
          *,

          CONCAT(
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', RaceDateTime),
            '|',
            CAST(Pre_RaceLocation AS STRING)
          ) AS RaceID,

          REGEXP_CONTAINS(
            UPPER(COALESCE(RaceHistoryStats, '')),
            r'\\bBF\\b'
          ) AS BF,

          REGEXP_CONTAINS(
            UPPER(COALESCE(RaceHistoryStats, '')),
            r'\\bCD\\b'
          ) AS CD,

          (
            REGEXP_CONTAINS(
              UPPER(COALESCE(RaceHistoryStats, '')),
              r'(?:^|[^A-Z])C(?:[^A-Z]|$)'
            )
            AND NOT REGEXP_CONTAINS(
              UPPER(COALESCE(RaceHistoryStats, '')),
              r'\\bCD\\b'
            )
          ) AS C,

          (
            REGEXP_CONTAINS(
              UPPER(COALESCE(RaceHistoryStats, '')),
              r'(?:^|[^A-Z])D(?:[^A-Z]|$)'
            )
            AND NOT REGEXP_CONTAINS(
              UPPER(COALESCE(RaceHistoryStats, '')),
              r'\\bCD\\b'
            )
          ) AS D,

          STARTS_WITH(
            LOWER(
              TRIM(
                REGEXP_REPLACE(
                  COALESCE(Pre_RaceGoing, ''),
                  r'\\s*\$begin:math:text$\.\*\?\\$end:math:text$',
                  ''
                )
              )
            ),
            'standard'
          ) AS Going_Standard,

          LOWER(COALESCE(Favourite, '')) = 'f' AS fav,

          SAFE_CAST(LastRun AS FLOAT64) AS LastRunDays

        FROM source
      ),

      qualified AS (
        SELECT *
        FROM features
        WHERE ${whereConditions}
      ),

      unique_selections AS (
        SELECT *
        FROM qualified
        QUALIFY COUNT(*) OVER (
          PARTITION BY RaceID
        ) = 1
      ),

      daily AS (
        SELECT
          RaceDateDt AS Date,
          COUNT(*) AS Bets,
          COUNTIF(Result_Position = 1) AS Winners,
          SUM(Staked) AS Staked,
          SUM(Total_Returns) AS Returns,
          SUM(Total_Returns - Staked) AS Profit
        FROM unique_selections
        GROUP BY RaceDateDt
      )

      SELECT *
      FROM daily
      ORDER BY Date
    `;

    const [rows] = await client.query({
      query,
      params: {
        startDate,
        endDate,
      },
      maximumBytesBilled: "10000000000",
    });

    let cumulativeProfit = 0;
    let cumulativeStaked = 0;

    const dailyPerformance = rows.map((row) => {
      const staked = Number(row.Staked);
      const profit = Number(row.Profit);

      cumulativeProfit += profit;
      cumulativeStaked += staked;

      return {
        date: dateString(row.Date),
        bets: Number(row.Bets),
        winners: Number(row.Winners),
        staked,
        returns: Number(row.Returns),
        profit,
        cumulativeProfit,
        cumulativeROI:
          cumulativeStaked > 0
            ? (cumulativeProfit / cumulativeStaked) * 100
            : 0,
      };
    });

    const bets = dailyPerformance.reduce(
      (sum, day) => sum + day.bets,
      0
    );

    const winners = dailyPerformance.reduce(
      (sum, day) => sum + day.winners,
      0
    );

    const staked = dailyPerformance.reduce(
      (sum, day) => sum + day.staked,
      0
    );

    const returns = dailyPerformance.reduce(
      (sum, day) => sum + day.returns,
      0
    );

    const profit = returns - staked;

    let peak = 0;
    let maxDrawdown = 0;

    for (const day of dailyPerformance) {
      peak = Math.max(peak, day.cumulativeProfit);

      maxDrawdown = Math.max(
        maxDrawdown,
        peak - day.cumulativeProfit
      );
    }

    return NextResponse.json({
      strategy: {
        runTimestamp: timestampString(strategy.RunTimestamp),
        rank: Number(strategy.Rank),
        conditions: String(strategy.Conditions),
      },

      period: {
        startDate,
        endDate,
      },

      summary: {
        bets,
        winners,
        strikeRate: bets > 0 ? (winners / bets) * 100 : 0,
        staked,
        returns,
        profit,
        roi: staked > 0 ? (profit / staked) * 100 : 0,
        maxDrawdown,
      },

      dailyPerformance,
    });
  } catch (error) {
    unstable_rethrow(error);

    console.error("Modelled scenario backtest failed:", error);

    return NextResponse.json(
      { error: "Unable to calculate modelled scenario performance." },
      { status: 500 }
    );
  }
}