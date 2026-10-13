import { getExchangeRate } from "@/lib/currency";
import { getLast12MonthStats, type Last12MonthEntityType } from "@/lib/raceViewer";
import { NextRequest, NextResponse } from "next/server";

const validEntityTypes: Last12MonthEntityType[] = ["HORSE", "JOCKEY", "TRAINER"];

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const names = body.names as string[];
    const asOfDate = body.asOfDate as string;
    const entityType = body.entityType as Last12MonthEntityType;

    if (!Array.isArray(names) || names.length === 0 || names.some((name) => typeof name !== "string")) {
      return NextResponse.json({ error: "At least one valid entity name is required." }, { status: 400 });
    }
    if (typeof asOfDate !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(asOfDate)) {
      return NextResponse.json({ error: "A valid as-of date is required." }, { status: 400 });
    }
    if (!validEntityTypes.includes(entityType)) {
      return NextResponse.json({ error: "Invalid entity type." }, { status: 400 });
    }

    const exchangeRate = await getExchangeRate();
    const stats = await getLast12MonthStats(names, asOfDate, entityType, exchangeRate.eurToGbp);
    return NextResponse.json({ stats, exchangeRate });
  } catch (error) {
    console.error("Last 12 months API error:", error);
    return NextResponse.json({ error: error instanceof Error ? error.message : String(error) }, { status: 500 });
  }
}
