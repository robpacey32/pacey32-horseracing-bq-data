import { NextRequest, NextResponse } from "next/server";
import { unstable_rethrow } from "next/navigation";
import { getBigQueryClient } from "@/lib/bigquery";
import { TABLE } from "@/lib/scenarios/apiCommon";

const COLUMNS = {
  horse: "HorseName",
  jockey: "Jockey",
  trainer: "Trainer",
} as const;

const PRE_RACE_TABLE =
  "`horseracing-pacey32-github.horseraceanalytics.Scenario_PreRace_Data_vw`";

type NameRow = { name: string };

export async function GET(request: NextRequest) {
  const kind = request.nextUrl.searchParams.get("kind");
  const search = (request.nextUrl.searchParams.get("q") ?? "").trim();

  if (!kind || !Object.hasOwn(COLUMNS, kind)) {
    return NextResponse.json({ error: "Choose horses, jockeys or trainers." }, { status: 400 });
  }

  if (search.length > 100) {
    return NextResponse.json({ error: "Search must contain at most 100 characters." }, { status: 400 });
  }

  // Short input never starts a BigQuery lookup.
  if (search.length < 2) {
    return NextResponse.json({ names: [], hasMore: false });
  }

  // The column comes exclusively from this fixed list, not from user SQL.
  const column = COLUMNS[kind as keyof typeof COLUMNS];

  try {
    const query = `
      WITH candidates AS (
        SELECT TRIM(CAST(${column} AS STRING)) AS name
        FROM ${TABLE}
        WHERE STRPOS(LOWER(CAST(${column} AS STRING)), @search) > 0

        UNION ALL

        SELECT TRIM(CAST(${column} AS STRING)) AS name
        FROM ${PRE_RACE_TABLE}
        WHERE STRPOS(LOWER(CAST(${column} AS STRING)), @search) > 0
      ), names AS (
        SELECT MIN(name) AS name
        FROM candidates
        WHERE name IS NOT NULL
          AND LENGTH(name) BETWEEN 1 AND 100
          AND LOWER(name) NOT IN ('n/a', 'na', 'unknown', '-')
        GROUP BY LOWER(name)
      )
      SELECT name
      FROM names
      ORDER BY
        CASE
          WHEN LOWER(name) = @search THEN 0
          WHEN STARTS_WITH(LOWER(name), @search) THEN 1
          ELSE 2
        END,
        LOWER(name),
        name
      LIMIT 51
    `;

    const [rows] = await getBigQueryClient().query({
      query,
      params: { search: search.toLowerCase() },
      maximumBytesBilled: "10000000000",
    });

    const names = (rows as NameRow[]).map((row) => row.name);

    return NextResponse.json(
      { names: names.slice(0, 50), hasMore: names.length > 50 },
      { headers: { "Cache-Control": "public, max-age=300" } }
    );
  } catch (error) {
    unstable_rethrow(error);
    console.error("Scenario name lookup error:", error);
    return NextResponse.json(
      { error: "Could not load names. Please try again." },
      { status: 500 }
    );
  }
}
