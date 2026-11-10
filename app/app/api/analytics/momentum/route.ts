
import { NextRequest, NextResponse } from "next/server";
import { connection } from "next/server";
import {
  getBigQueryClient,
  PROJECT_ID,
  ANALYTICS_DATASET,
} from "@/lib/bigquery";
import { getExchangeRate } from "@/lib/currency";

type EntityType = "HORSE" | "TRAINER" | "JOCKEY";
type Currency = "GBP" | "EUR";

export async function GET(request: NextRequest) {
  await connection();

  try {
    const params = request.nextUrl.searchParams;

    const entityType = params.get("entityType") ?? "TRAINER";
    const currency = params.get("currency") ?? "GBP";
    const months = Number(params.get("months") ?? "1");
    const endDate =
      params.get("endDate") ??
      new Date().toISOString().slice(0, 10);

    if (
      !["HORSE", "TRAINER", "JOCKEY"].includes(entityType) ||
      !["GBP", "EUR"].includes(currency) ||
      ![1, 3, 6].includes(months) ||
      !/^\d{4}-\d{2}-\d{2}$/.test(endDate) ||
      !Number.isFinite(Date.parse(`${endDate}T00:00:00Z`))
    ) {
      return NextResponse.json(
        { error: "Invalid query parameters" },
        { status: 400 }
      );
    }

    const query = `
      WITH periods AS (
        SELECT
          DATE_ADD(
            DATE_SUB(CAST(@end_date AS DATE), INTERVAL @months MONTH),
            INTERVAL 1 DAY
          ) AS current_start,
          DATE_SUB(CAST(@end_date AS DATE), INTERVAL @months MONTH)
            AS previous_end,
          DATE_ADD(
            DATE_SUB(
              CAST(@end_date AS DATE),
              INTERVAL (2 * @months) MONTH
            ),
            INTERVAL 1 DAY
          ) AS previous_start
      ),
      race_data AS (
        SELECT
          CASE
            WHEN @entity_type = 'HORSE' THEN HorseName
            WHEN @entity_type = 'TRAINER' THEN Trainer
            ELSE Jockey
          END AS Entity,
          CASE
            WHEN RaceDate >= p.current_start THEN 'current'
            ELSE 'previous'
          END AS Period,
          PosInt,
          Placed,
          RaceRunners,
          PrizeMoneyNumeric,
          CASE
            WHEN PosInt IS NOT NULL
              AND RaceRunners > 1
            THEN 100 * (
              1 - SAFE_DIVIDE(PosInt - 1, RaceRunners - 1)
            )
            ELSE NULL
          END AS FinishScore
        FROM \`${PROJECT_ID}.${ANALYTICS_DATASET}.RaceTotalsForApp\`
        CROSS JOIN periods p
        WHERE RaceDate BETWEEN p.previous_start
                           AND CAST(@end_date AS DATE)
      ),
      aggregated AS (
        SELECT
          Entity,
          Period,
          COUNT(*) AS runs,
          COUNTIF(PosInt = 1) AS wins,
          COUNTIF(PosInt = 1 OR Placed = 1) AS places,
          AVG(FinishScore) AS finishScore,
          SUM(COALESCE(PrizeMoneyNumeric, 0))
            AS prizeMoney
        FROM race_data
        WHERE Entity IS NOT NULL
          AND TRIM(Entity) != ''
        GROUP BY Entity, Period
      )
      SELECT
        Entity AS name,
        Period AS period,
        runs,
        wins,
        places,
        ROUND(SAFE_DIVIDE(wins, runs) * 100, 2)
          AS winRate,
        ROUND(SAFE_DIVIDE(places, runs) * 100, 2)
          AS placeRate,
        ROUND(finishScore, 2) AS finishScore,
        prizeMoney
      FROM aggregated
      ORDER BY Entity, Period
    `;

    const bigquery = getBigQueryClient();

    const [results] = await bigquery.query({
      query,
      params: {
        entity_type: entityType,
        months,
        end_date: endDate,
      },
    });

    const exchangeRate = await getExchangeRate();
    const multiplier =
      currency === "EUR" ? exchangeRate.gbpToEur : 1;

    type PeriodRow = {
      name: string;
      period: "current" | "previous";
      runs: number;
      wins: number;
      places: number;
      winRate: number;
      placeRate: number;
      finishScore: number | null;
      prizeMoney: number;
    };

    const grouped = new Map<
      string,
      { current?: PeriodRow; previous?: PeriodRow }
    >();

    for (const raw of results) {
      const row = raw as PeriodRow;

      const existing = grouped.get(row.name) ?? {};
      existing[row.period] = row;
      grouped.set(row.name, existing);
    }

    const rows = Array.from(grouped.entries()).map(
      ([name, periods]) => {
        const convert = (row?: PeriodRow) => {
          if (!row) {
            return {
              runs: 0,
              wins: 0,
              places: 0,
              winRate: 0,
              placeRate: 0,
              finishScore: null,
              prizeMoney: 0,
              prizePerRun: 0,
            };
          }

          const prizeMoney =
            Number(row.prizeMoney) * multiplier;

          return {
            runs: Number(row.runs),
            wins: Number(row.wins),
            places: Number(row.places),
            winRate: Number(row.winRate),
            placeRate: Number(row.placeRate),
            finishScore:
              row.finishScore == null
                ? null
                : Number(row.finishScore),
            prizeMoney,
            prizePerRun:
              Number(row.runs) > 0
                ? prizeMoney / Number(row.runs)
                : 0,
          };
        };

        const current = convert(periods.current);
        const previous = convert(periods.previous);

        return {
          name,
          current,
          previous,
          change: {
            winRate: current.winRate - previous.winRate,
            placeRate: current.placeRate - previous.placeRate,
            finishScore:
              current.finishScore == null ||
              previous.finishScore == null
                ? null
                : current.finishScore -
                  previous.finishScore,
            prizePerRun:
              current.prizePerRun - previous.prizePerRun,
          },
        };
      }
    );

    const end = new Date(`${endDate}T12:00:00Z`);

    function shiftMonths(date: Date, count: number) {
      const year = date.getUTCFullYear();
      const month = date.getUTCMonth() + count;
      const day = date.getUTCDate();

      const first = new Date(
        Date.UTC(year, month, 1, 12)
      );
      const lastDay = new Date(
        Date.UTC(
          first.getUTCFullYear(),
          first.getUTCMonth() + 1,
          0,
          12
        )
      ).getUTCDate();

      first.setUTCDate(Math.min(day, lastDay));
      return first;
    }

    function formatDate(date: Date) {
      return date.toISOString().slice(0, 10);
    }

    const currentBoundary = shiftMonths(end, -months);
    const previousBoundary = shiftMonths(end, -2 * months);

    const currentStart = new Date(currentBoundary);
    currentStart.setUTCDate(currentStart.getUTCDate() + 1);

    const previousStart = new Date(previousBoundary);
    previousStart.setUTCDate(previousStart.getUTCDate() + 1);

    return NextResponse.json({
      entityType: entityType as EntityType,
      currency: currency as Currency,
      months,
      endDate,
      periods: {
        current: {
          start: formatDate(currentStart),
          end: endDate,
        },
        previous: {
          start: formatDate(previousStart),
          end: formatDate(currentBoundary),
        },
      },
      exchangeRate,
      rows,
    });
  } catch (error) {
    console.error("Momentum API error:", error);

    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Failed to load momentum analytics",
      },
      { status: 500 }
    );
  }
}
