import { NextResponse } from "next/server";
import { unstable_rethrow } from "next/navigation";
import { getBigQueryClient } from "@/lib/bigquery";
import { TABLE } from "@/lib/scenarios/apiCommon";

type OptionRow = { category: string; value: string };

export async function GET() {
  try {
    const query = `
      WITH values AS (
        SELECT category, TRIM(value) AS value
        FROM ${TABLE}, UNNEST([
          STRUCT('courses' AS category, CAST(Pre_RaceLocation AS STRING) AS value),
          STRUCT('raceClasses' AS category, CAST(Pre_RaceClass AS STRING) AS value),
          STRUCT('goings' AS category, CAST(Pre_RaceGoing AS STRING) AS value),
          STRUCT('surfaces' AS category, CAST(Pre_RaceSurface AS STRING) AS value)
        ])
      )
      SELECT category, value FROM values
      WHERE value IS NOT NULL AND value != ''
      GROUP BY category, value
      ORDER BY category, value
    `;
    const [rows] = await getBigQueryClient().query({ query, maximumBytesBilled: "10000000000" });
    const options: Record<string, string[]> = { courses: [], raceClasses: [], goings: [], surfaces: [] };
    for (const row of rows as OptionRow[]) {
      if (Object.hasOwn(options, row.category)) options[row.category].push(row.value);
    }
    for (const key of Object.keys(options)) options[key].sort((a, b) => a.localeCompare(b, "en-GB"));
    return NextResponse.json(options, { headers: { "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400" } });
  } catch (error) {
    unstable_rethrow(error);
    console.error("Scenario filter options error:", error);
    return NextResponse.json({ error: "Could not load filter options." }, { status: 500 });
  }
}
