
import { NextRequest, NextResponse } from "next/server";
import { getBigQueryClient, PROJECT_ID, ANALYTICS_DATASET } from "@/lib/bigquery";
import { getExchangeRate } from "@/lib/currency";
import { connection } from "next/server";

type EntityType = "HORSE" | "TRAINER" | "JOCKEY";
type Currency = "GBP" | "EUR";

export async function GET(request: NextRequest) {
    await connection();
    
  try {
    const params = request.nextUrl.searchParams;

    const entityType = (params.get("entityType") ?? "TRAINER").toUpperCase();
    const currency = (params.get("currency") ?? "GBP").toUpperCase();
    const startDate = params.get("startDate") ?? "2025-01-01";
    const endDate = params.get("endDate") ?? new Date().toISOString().slice(0, 10);

    if (!["HORSE", "TRAINER", "JOCKEY"].includes(entityType)) {
      return NextResponse.json({ error: "Invalid entity type." }, { status: 400 });
    }

    if (!["GBP", "EUR"].includes(currency)) {
      return NextResponse.json({ error: "Invalid currency." }, { status: 400 });
    }

    if (
      !/^\d{4}-\d{2}-\d{2}$/.test(startDate) ||
      !/^\d{4}-\d{2}-\d{2}$/.test(endDate) ||
      startDate > endDate
    ) {
      return NextResponse.json({ error: "Invalid date range." }, { status: 400 });
    }

    const exchangeRate = await getExchangeRate();

    const query = `
      WITH results AS (
        SELECT
          CASE
            WHEN @entity_type = 'HORSE' THEN HorseName
            WHEN @entity_type = 'TRAINER' THEN Trainer
            WHEN @entity_type = 'JOCKEY' THEN Jockey
          END AS Entity,
          PosInt,
          RaceRunners,
          Placed,
          PrizeMoney,
          PrizeMoneyNumeric
        FROM \`${PROJECT_ID}.${ANALYTICS_DATASET}.RaceTotalsForApp\`
        WHERE RaceDate BETWEEN CAST(@start_date AS DATE)
                   AND CAST(@end_date AS DATE)
      ),
      prepared AS (
        SELECT
          Entity,
          PosInt,
          RaceRunners,
          IF(PosInt = 1 OR Placed = 1, 1, 0) AS PaidPlace,
          CASE
            WHEN STRPOS(PrizeMoney, '£') > 0
              THEN COALESCE(PrizeMoneyNumeric, 0)
            WHEN STRPOS(PrizeMoney, '€') > 0
              THEN COALESCE(PrizeMoneyNumeric, 0) * @eur_to_gbp
            ELSE 0
          END AS PrizeGBP
        FROM results
        WHERE Entity IS NOT NULL AND TRIM(Entity) != ''
      )
      SELECT
        Entity AS name,
        COUNT(*) AS runs,
        COUNTIF(PosInt = 1) AS wins,
        COUNTIF(PaidPlace = 1) AS places,
        ROUND(100 * SAFE_DIVIDE(COUNTIF(PosInt = 1), COUNT(*)), 2) AS winRate,
        ROUND(100 * SAFE_DIVIDE(COUNTIF(PaidPlace = 1), COUNT(*)), 2) AS placeRate,
        ROUND(AVG(PosInt), 2) AS averageFinish,
        ROUND(
          AVG(
            IF(
              PosInt IS NOT NULL AND RaceRunners > 1,
              100 * SAFE_DIVIDE(RaceRunners - PosInt, RaceRunners - 1),
              NULL
            )
          ),
          2
        ) AS averageFinishPercentile,
        ROUND(SUM(PrizeGBP) * @currency_multiplier, 2) AS prizeMoney
      FROM prepared
      GROUP BY Entity
      ORDER BY prizeMoney DESC, wins DESC, runs DESC
      LIMIT 500
    `;

    const [rows] = await getBigQueryClient().query({
      query,
      params: {
        entity_type: entityType as EntityType,
        start_date: startDate,
        end_date: endDate,
        eur_to_gbp: exchangeRate.eurToGbp,
        currency_multiplier:
          currency === "EUR" ? exchangeRate.gbpToEur : 1,
      },
      types: {
        entity_type: "STRING",
        start_date: "STRING",
        end_date: "STRING",
        eur_to_gbp: "FLOAT64",
        currency_multiplier: "FLOAT64",
      },
    });

    return NextResponse.json({
      entityType,
      currency: currency as Currency,
      startDate,
      endDate,
      exchangeRate,
      rows: rows.map((row) => ({
        name: String(row.name ?? ""),
        runs: Number(row.runs ?? 0),
        wins: Number(row.wins ?? 0),
        places: Number(row.places ?? 0),
        winRate: Number(row.winRate ?? 0),
        placeRate: Number(row.placeRate ?? 0),
        averageFinish: row.averageFinish == null ? null : Number(row.averageFinish),
        averageFinishPercentile:
          row.averageFinishPercentile == null
            ? null
            : Number(row.averageFinishPercentile),
        prizeMoney: Number(row.prizeMoney ?? 0),
      })),
    });
  } catch (error) {
    console.error("Racing Analytics leaderboard error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : String(error) },
      { status: 500 }
    );
  }
}
