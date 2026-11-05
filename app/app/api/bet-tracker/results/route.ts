import { NextRequest, NextResponse } from "next/server";
import { unstable_rethrow } from "next/navigation";
import { getBigQueryClient, PROJECT_ID } from "@/lib/bigquery";

const MAX_RUNNERS = 100;

type ResultsRequest = {
  runnerIds?: unknown;
};

export async function POST(request: NextRequest) {
  try {
    let body: ResultsRequest;
    try {
      body = (await request.json()) as ResultsRequest;
    } catch {
      return NextResponse.json({ error: "Invalid JSON body." }, { status: 400 });
    }

    if (
      !Array.isArray(body.runnerIds) ||
      body.runnerIds.length === 0 ||
      body.runnerIds.length > MAX_RUNNERS ||
      !body.runnerIds.every(
        (id): id is string => typeof id === "string" && /^[a-fA-F0-9]{32}$/.test(id)
      )
    ) {
      return NextResponse.json(
        { error: `runnerIds must contain 1–${MAX_RUNNERS} valid runner IDs.` },
        { status: 400 }
      );
    }

    const runnerIds = [...new Set(body.runnerIds)];

    // The cleansed view calculates runner_id in the next layer using
    // the same concatenation of race fields and horse name.
    const query = `
      WITH results AS (
        SELECT
          TO_HEX(MD5(CONCAT(
            COALESCE(Pre_RaceDate, Post_RaceDate), '|',
            COALESCE(Pre_RaceLocation, Post_RaceLocation), '|',
            COALESCE(Pre_RaceTime, Post_RaceTime), '|',
            COALESCE(Pre_RaceName, Post_RaceName), '|',
            HorseName
          ))) AS runner_id,
          HorseName AS horseName,
          Result AS result,
          Result_Position AS resultPosition,
          Odds AS originalOdds,
          place_terms AS placeTerms,
          places_paid AS placesPaid,
          ew_possible AS ewPossible,
          Post_RaceRunners AS actualRunners
        FROM \`${PROJECT_ID}.bettingapp.1_CleansedRaceData\`
      )
      SELECT *
      FROM results
      WHERE runner_id IN UNNEST(@runnerIds)
    `;

    const [rows] = await getBigQueryClient().query({
      query,
      params: { runnerIds },
      types: { runnerIds: ["STRING"] },
      maximumBytesBilled: "1000000000", // 1 GB safety cap per request
    });

    return NextResponse.json({ count: rows.length, results: rows });
  } catch (error) {
    unstable_rethrow(error);
    console.error("Bet Tracker results API error:", error);
    return NextResponse.json(
      { error: "Failed to retrieve betting results." },
      { status: 500 }
    );
  }
}
