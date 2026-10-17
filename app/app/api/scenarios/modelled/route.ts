import { NextRequest, NextResponse } from "next/server";
import { unstable_rethrow } from "next/navigation";
import { getBigQueryClient } from "@/lib/bigquery";

const TABLE = "`horseracing-pacey32-github.horseraceanalytics.ScenarioTop20_History`";

function timestampString(value: unknown): string {
  if (value instanceof Date) return value.toISOString();
  if (value && typeof value === "object" && "value" in value) {
    return timestampString((value as { value: unknown }).value);
  }
  return String(value ?? "");
}

export async function GET(request: NextRequest) {
  try {
    const client = getBigQueryClient();
    const requestedRun = request.nextUrl.searchParams.get("run");

    if (requestedRun && !Number.isFinite(Date.parse(requestedRun))) {
      return NextResponse.json({ error: "Invalid model run timestamp." }, { status: 400 });
    }

    const [runRows] = await client.query({
      query: `SELECT RunTimestamp, COUNT(*) AS Strategies
              FROM ${TABLE}
              GROUP BY RunTimestamp
              ORDER BY RunTimestamp DESC
              LIMIT 100`,
      maximumBytesBilled: "1000000000",
    });

    const runs = runRows.map((row) => ({
      timestamp: timestampString(row.RunTimestamp),
      strategies: Number(row.Strategies),
    }));

    const selectedRun = requestedRun ?? runs[0]?.timestamp;
    if (!selectedRun) return NextResponse.json({ runs: [], selectedRun: null, strategies: [] });

    const [rows] = await client.query({
      query: `SELECT RunTimestamp, LookbackMonths, Rank, Conditions, Bets,
                     BetsPerDay, BetsPerWeek, ActiveDays, PctDaysWithBet,
                     Profit, ProfitPerBet, ProfitPerDay, ROI, StrikeRate
              FROM ${TABLE}
              WHERE TIMESTAMP_TRUNC(RunTimestamp, SECOND)
                  = TIMESTAMP_TRUNC(@runTimestamp, SECOND)
              ORDER BY Rank ASC
              LIMIT 20`,
      params: { runTimestamp: new Date(selectedRun) },
      maximumBytesBilled: "1000000000",
    });

    return NextResponse.json({
      runs,
      selectedRun,
      strategies: rows.map((row) => ({
        runTimestamp: timestampString(row.RunTimestamp),
        lookbackMonths: Number(row.LookbackMonths),
        rank: Number(row.Rank),
        conditions: String(row.Conditions),
        bets: Number(row.Bets),
        betsPerWeek: Number(row.BetsPerWeek),
        activeDays: Number(row.ActiveDays),
        profit: Number(row.Profit),
        profitPerBet: Number(row.ProfitPerBet),
        profitPerDay: Number(row.ProfitPerDay),
        modelRoi: Number(row.ROI),
        strikeRate: Number(row.StrikeRate),
      })),
    });
  } catch (error) {
    unstable_rethrow(error);
    console.error("Modelled scenarios query failed", error);
    return NextResponse.json({ error: "Unable to load modelled scenarios." }, { status: 500 });
  }
}
