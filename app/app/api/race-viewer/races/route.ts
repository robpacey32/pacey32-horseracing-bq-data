import { NextRequest, NextResponse } from "next/server";
import { getRaceDates, getRacesForDate } from "@/lib/raceViewer";
import { unstable_rethrow } from "next/navigation";

export async function GET(request: NextRequest) {
  try {
    const date = request.nextUrl.searchParams.get("date");

    if (!date) {
      const dates = await getRaceDates();

      return NextResponse.json({
        dates,
      });
    }

    const races = await getRacesForDate(date);

    return NextResponse.json({
      races,
    });
  } catch (error) {
    unstable_rethrow(error);
    
    console.error("Race Viewer API error:", error);

    const message =
      error instanceof Error ? error.message : String(error);

    return NextResponse.json(
      {
        error: message,
      },
      { status: 500 }
    );
  }
}