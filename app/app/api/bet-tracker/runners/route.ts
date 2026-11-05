import { NextRequest, NextResponse } from "next/server";
import { getBigQueryClient, PROJECT_ID } from "@/lib/bigquery";
import { unstable_rethrow } from "next/navigation";

export async function GET(request: NextRequest) {
  try {
    const date = request.nextUrl.searchParams.get("date");

    // Require a valid date
    if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return NextResponse.json(
        { error: "A valid date (YYYY-MM-DD) is required." },
        { status: 400 }
      );
    }

    const query = `
      SELECT
        race_id,
        runner_id,
        CAST(RaceDate AS STRING) AS RaceDate,
        RaceTime,
        RaceLocation,
        RaceName,
        HorseNumber,
        HorseName,
        Trainer,
        Jockey,
        Odds,
        Form,
        RaceClass,
        RaceDistance,
        RaceGoing,
        RaceRunners,
        RaceSurface,
        Result,
        PrizeMoney,
        Age,
        Weight,
        RaceHistoryStats,
        LastRun,
        Headgear,
        RaceType

      FROM \`${PROJECT_ID}.bettingapp.RunnerLatest_vw\`

      WHERE RaceDate = CAST(@date AS DATE)

      ORDER BY
        RaceTime,
        RaceLocation,
        RaceName,
        HorseName
    `;

    const [rows] = await getBigQueryClient().query({
      query,
      params: { date },
      types: { date: "STRING" },
    });

    return NextResponse.json({
      date,
      count: rows.length,
      runners: rows,
    });
  } catch (error) {
    unstable_rethrow(error);

    console.error("Bet Tracker runners API error:", error);

    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Failed to retrieve runners.",
      },
      { status: 500 }
    );
  }
}