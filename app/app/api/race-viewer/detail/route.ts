import { NextRequest, NextResponse } from "next/server";
import { getRaceDetail } from "@/lib/raceViewer";
import { unstable_rethrow } from "next/navigation";

export async function GET(request: NextRequest) {
  try {
    const date = request.nextUrl.searchParams.get("date");
    const location = request.nextUrl.searchParams.get("location");
    const time = request.nextUrl.searchParams.get("time");

    if (!date || !location || !time) {
      return NextResponse.json(
        {
          error: "Date, location and time are required.",
        },
        { status: 400 }
      );
    }

    const race = await getRaceDetail(date, location, time);

    if (!race) {
      return NextResponse.json(
        {
          error: "Race not found.",
        },
        { status: 404 }
      );
    }

    return NextResponse.json({
      race,
    });
  } catch (error) {
    unstable_rethrow(error);
    
    console.error("Race Viewer detail API error:", error);

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