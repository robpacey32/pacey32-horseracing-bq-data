
import { NextRequest, NextResponse, connection } from "next/server";
import {
  getBigQueryClient,
  PROJECT_ID,
  ANALYTICS_DATASET,
} from "@/lib/bigquery";
import { getExchangeRate } from "@/lib/currency";

type RawRow = {
  date: string;
  location: string | null;
  time: string | null;
  raceName: string | null;
  horse: string | null;
  trainer: string | null;
  jockey: string | null;
  position: string | null;
  positionNumber: number | null;
  runners: number | null;
  won: boolean;
  placed: boolean;
  finishScore: number | null;
  prizeMoney: number | null;

  raceType: string | null;
  going: string | null;
  surface: string | null;
  rideDescription: string | null;

  sp: string | null;
  favouriteStatus: string | null;
  courseDistance: string | null;
  beatenFavourite: string | null;
  raceDistance: string | null;
  raceClass: string | null;
  weight: string | null;
  age: string | null;
  headgear: string | null;
  lastRun: string | null;
  winningTime: string | null;
  sourceUrl: string | null;
};

function normaliseCourseDistance(
  value: string | null
): string | null {
  if (!value) return null;

  const cleaned = value.trim().toUpperCase().replace(/\s+/g, "");

  if (cleaned === "CD" || cleaned === "C&D") return "CD";
  if (cleaned === "C") return "C";
  if (cleaned === "D") return "D";

  return null;
}

function normaliseBeatenFavourite(
  value: string | null
): string | null {
  if (!value) return null;

  const cleaned = value.trim().toUpperCase();

  if (["TRUE", "YES", "Y", "1", "BF"].includes(cleaned)) {
    return "Yes";
  }

  if (["FALSE", "NO", "N", "0"].includes(cleaned)) {
    return "No";
  }

  return null;
}

export async function GET(request: NextRequest) {
  await connection();

  try {
    const params = request.nextUrl.searchParams;

    const type = params.get("type") ?? "";
    const name = params.get("name")?.trim() ?? "";
    const currency = params.get("currency") ?? "GBP";
    const startDate = params.get("startDate") ?? "2025-01-01";
    const endDate =
      params.get("endDate") ??
      new Date().toISOString().slice(0, 10);

    if (
      !["HORSE", "TRAINER", "JOCKEY"].includes(type) ||
      !name ||
      !["GBP", "EUR"].includes(currency) ||
      !/^\d{4}-\d{2}-\d{2}$/.test(startDate) ||
      !/^\d{4}-\d{2}-\d{2}$/.test(endDate) ||
      startDate > endDate ||
      !Number.isFinite(Date.parse(startDate)) ||
      !Number.isFinite(Date.parse(endDate))
    ) {
      return NextResponse.json(
        { error: "Invalid drilldown parameters" },
        { status: 400 }
      );
    }

    const query = `
      WITH selected AS (
        SELECT
          RaceDate,
          RaceLocation,
          RaceTime,
          RaceName,
          HorseName,
          Trainer,
          Jockey,
          Pos,
          PosInt,
          RaceRunners,
          Placed,
          COALESCE(PrizeMoneyNumeric, 0) AS PrizeMoneyNumeric,

          CASE
            WHEN PosInt IS NOT NULL
              AND RaceRunners > 1
            THEN 100 * (
              1 - SAFE_DIVIDE(PosInt - 1, RaceRunners - 1)
            )
            ELSE NULL
          END AS FinishScore

        FROM \`${PROJECT_ID}.${ANALYTICS_DATASET}.RaceTotalsForApp\`

        WHERE RaceDate BETWEEN CAST(@start_date AS DATE)
                           AND CAST(@end_date AS DATE)

          AND CASE
            WHEN @entity_type = 'HORSE' THEN HorseName
            WHEN @entity_type = 'TRAINER' THEN Trainer
            ELSE Jockey
          END = @entity_name
      ),

      scenario_deduplicated AS (
        SELECT
          SAFE.PARSE_DATE(
            '%d/%m/%Y',
            s.Post_RaceDate
          ) AS RaceDate,

          LOWER(TRIM(s.Post_RaceLocation)) AS LocationKey,
          TRIM(s.Post_RaceTime) AS TimeKey,
          LOWER(TRIM(s.Post_HorseName)) AS HorseKey,

          s.Pre_RaceType,
          s.Post_RaceGoing,
          s.Post_RaceSurface,
          s.RideDescription,

          TO_JSON_STRING(s) AS ScenarioJson

        FROM \`${PROJECT_ID}.${ANALYTICS_DATASET}.Scenario_App_Data_vw\` s

        WHERE SAFE.PARSE_DATE(
                '%d/%m/%Y',
                s.Post_RaceDate
              )
              BETWEEN CAST(@start_date AS DATE)
                  AND CAST(@end_date AS DATE)

          AND s.Post_HorseName IS NOT NULL

        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY
            SAFE.PARSE_DATE('%d/%m/%Y', s.Post_RaceDate),
            LOWER(TRIM(s.Post_RaceLocation)),
            TRIM(s.Post_RaceTime),
            LOWER(TRIM(s.Post_HorseName))

          ORDER BY
            CASE
              WHEN s.RideDescription IS NOT NULL THEN 0
              ELSE 1
            END,
            CASE
              WHEN s.Post_RaceGoing IS NOT NULL THEN 0
              ELSE 1
            END,
            CASE
              WHEN s.Post_RaceSurface IS NOT NULL THEN 0
              ELSE 1
            END
        ) = 1
      )

      SELECT
        CAST(r.RaceDate AS STRING) AS date,
        r.RaceLocation AS location,
        r.RaceTime AS time,
        r.RaceName AS raceName,
        r.HorseName AS horse,
        r.Trainer AS trainer,
        r.Jockey AS jockey,
        r.Pos AS position,
        r.PosInt AS positionNumber,
        r.RaceRunners AS runners,

        IF(r.PosInt = 1, TRUE, FALSE) AS won,
        IF(
          r.PosInt = 1 OR r.Placed = 1,
          TRUE,
          FALSE
        ) AS placed,

        r.FinishScore AS finishScore,
        r.PrizeMoneyNumeric AS prizeMoney,

        s.Pre_RaceType AS raceType,
        s.Post_RaceGoing AS going,
        s.Post_RaceSurface AS surface,
        s.RideDescription AS rideDescription,

        COALESCE(
          JSON_VALUE(s.ScenarioJson, '$.SP'),
          JSON_VALUE(s.ScenarioJson, '$.Post_SP')
        ) AS sp,

        COALESCE(
          JSON_VALUE(s.ScenarioJson, '$.FavouriteStatus'),
          JSON_VALUE(s.ScenarioJson, '$.Favourite'),
          JSON_VALUE(s.ScenarioJson, '$.Pre_Favourite')
        ) AS favouriteStatus,

        COALESCE(
          JSON_VALUE(s.ScenarioJson, '$.CourseDistance'),
          JSON_VALUE(s.ScenarioJson, '$.CD')
        ) AS courseDistance,

        COALESCE(
          JSON_VALUE(s.ScenarioJson, '$.BF'),
          JSON_VALUE(s.ScenarioJson, '$.BeatenFavourite')
        ) AS beatenFavourite,

        COALESCE(
          JSON_VALUE(s.ScenarioJson, '$.Post_RaceDistance'),
          JSON_VALUE(s.ScenarioJson, '$.Pre_RaceDistance')
        ) AS raceDistance,

        JSON_VALUE(
          s.ScenarioJson,
          '$.Pre_RaceClass'
        ) AS raceClass,

        COALESCE(
          JSON_VALUE(s.ScenarioJson, '$.Weight'),
          JSON_VALUE(s.ScenarioJson, '$.Pre_Weight')
        ) AS weight,

        COALESCE(
          JSON_VALUE(s.ScenarioJson, '$.Age'),
          JSON_VALUE(s.ScenarioJson, '$.Pre_Age')
        ) AS age,

        COALESCE(
          JSON_VALUE(s.ScenarioJson, '$.Headgear'),
          JSON_VALUE(s.ScenarioJson, '$.Pre_Headgear')
        ) AS headgear,

        COALESCE(
          JSON_VALUE(s.ScenarioJson, '$.LastRun'),
          JSON_VALUE(s.ScenarioJson, '$.Pre_LastRun')
        ) AS lastRun,

        JSON_VALUE(
          s.ScenarioJson,
          '$.Post_WinningTime'
        ) AS winningTime,

        JSON_VALUE(
          s.ScenarioJson,
          '$.Post_SourceURL'
        ) AS sourceUrl

      FROM selected r

      LEFT JOIN scenario_deduplicated s
        ON r.RaceDate = s.RaceDate
        AND LOWER(TRIM(r.RaceLocation)) = s.LocationKey
        AND TRIM(r.RaceTime) = s.TimeKey
        AND LOWER(TRIM(r.HorseName)) = s.HorseKey

      ORDER BY
        r.RaceDate DESC,
        r.RaceTime DESC
    `;

    const bigquery = getBigQueryClient();

    const [results] = await bigquery.query({
      query,
      params: {
        entity_type: type,
        entity_name: name,
        start_date: startDate,
        end_date: endDate,
      },
    });

    const exchangeRate = await getExchangeRate();

    const multiplier =
      currency === "EUR" ? exchangeRate.gbpToEur : 1;

    const races = (results as RawRow[]).map((row) => ({
      ...row,

      prizeMoney:
        Number(row.prizeMoney ?? 0) * multiplier,

      finishScore:
        row.finishScore == null
          ? null
          : Number(row.finishScore),

      courseDistance: normaliseCourseDistance(
        row.courseDistance
      ),

      beatenFavourite: normaliseBeatenFavourite(
        row.beatenFavourite
      ),
    }));

    // Overall statistics
    const runs = races.length;

    const wins = races.filter(
      (race) => race.won
    ).length;

    const seconds = races.filter(
      (race) => race.positionNumber === 2
    ).length;

    const places = races.filter(
      (race) => race.placed
    ).length;

    const prizeMoney = races.reduce(
      (sum, race) => sum + race.prizeMoney,
      0
    );

    const validFinishes = races
      .map((race) => race.finishScore)
      .filter(
        (value): value is number => value !== null
      );

    const averageFinishScore = validFinishes.length
      ? validFinishes.reduce(
          (sum, value) => sum + value,
          0
        ) / validFinishes.length
      : null;

    // Monthly performance
    type MonthlyData = {
      month: string;
      runs: number;
      wins: number;
      places: number;
      prizeMoney: number;
    };

    const monthly = new Map<string, MonthlyData>();

    const cursor = new Date(
      `${startDate.slice(0, 7)}-01T00:00:00Z`
    );

    const lastMonth = endDate.slice(0, 7);

    // Include months without races.
    while (
      cursor.toISOString().slice(0, 7) <= lastMonth
    ) {
      const month = cursor.toISOString().slice(0, 7);

      monthly.set(month, {
        month,
        runs: 0,
        wins: 0,
        places: 0,
        prizeMoney: 0,
      });

      cursor.setUTCMonth(
        cursor.getUTCMonth() + 1
      );
    }

    for (const race of races) {
      const month = race.date.slice(0, 7);
      const item = monthly.get(month);

      if (!item) continue;

      item.runs += 1;
      item.wins += race.won ? 1 : 0;
      item.places += race.placed ? 1 : 0;
      item.prizeMoney += race.prizeMoney;
    }

    const trends = Array.from(monthly.values()).map(
      (item) => {
        const [year, month] = item.month
          .split("-")
          .map(Number);

        const monthLabel = new Intl.DateTimeFormat(
          "en-GB",
          {
            month: "short",
            year: "2-digit",
            timeZone: "UTC",
          }
        ).format(
          new Date(Date.UTC(year, month - 1, 1))
        );

        return {
          ...item,

          monthLabel: monthLabel.replace(" ", ""),

          winRate: item.runs
            ? (item.wins / item.runs) * 100
            : null,

          placeRate: item.runs
            ? (item.places / item.runs) * 100
            : null,

          prizePerRun: item.runs
            ? item.prizeMoney / item.runs
            : null,
        };
      }
    );

    return NextResponse.json({
      type,
      name,
      currency,
      startDate,
      endDate,
      exchangeRate,

      summary: {
        runs,
        wins,
        seconds,
        places,

        winRate: runs
          ? (wins / runs) * 100
          : 0,

        placeRate: runs
          ? (places / runs) * 100
          : 0,

        finishScore: averageFinishScore,
        prizeMoney,

        prizePerRun: runs
          ? prizeMoney / runs
          : 0,
      },

      trends,
      races,
    });
  } catch (error) {
    console.error(
      "Analytics drilldown error:",
      error
    );

    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Failed to load drilldown",
      },
      { status: 500 }
    );
  }
}
