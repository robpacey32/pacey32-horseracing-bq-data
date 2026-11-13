
import { NextRequest, NextResponse } from "next/server";
import { adminAuth, adminDb } from "@/lib/firebaseAdmin";

export async function GET(request: NextRequest) {
  try {
    const authorization =
      request.headers.get("authorization");

    if (!authorization?.startsWith("Bearer ")) {
      return NextResponse.json(
        { error: "Authentication required." },
        { status: 401 }
      );
    }

    const idToken = authorization.slice(7).trim();

    const decoded = await adminAuth.verifyIdToken(
      idToken,
      true
    );

    const link = await adminDb
      .collection("telegramAccountLinks")
      .doc(decoded.uid)
      .get();

    const data = link.data();

    const connected =
      link.exists &&
      typeof data?.telegramUserID === "string" &&
      data.telegramUserID.length > 0 &&
      typeof data?.chatID === "string" &&
      data.chatID.length > 0;

    return NextResponse.json(
      { connected },
      {
        headers: {
          "Cache-Control": "private, no-store",
        },
      }
    );
  } catch (error) {
    console.error(
      "Telegram connection status error:",
      error
    );

    return NextResponse.json(
      {
        error: "Unable to check Telegram connection.",
      },
      { status: 500 }
    );
  }
}
