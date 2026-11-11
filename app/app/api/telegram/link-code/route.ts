
import { randomBytes, createHash } from "node:crypto";
import { NextRequest, NextResponse } from "next/server";
import { adminAuth, adminDb } from "@/lib/firebaseAdmin";
import { FieldValue, Timestamp } from "firebase-admin/firestore";

const COLLECTION = "telegramLinkCodes";
const EXPIRY_SECONDS = 600;

export async function POST(request: NextRequest) {
  try {
    const authorization = request.headers.get("authorization");

    if (!authorization?.startsWith("Bearer ")) {
      return NextResponse.json(
        { error: "Authentication required." },
        { status: 401 }
      );
    }

    const idToken = authorization.slice(7).trim();
    const decoded = await adminAuth.verifyIdToken(idToken, true);
    const firebaseUID = decoded.uid;

    // Generate a cryptographically secure, 64-character linking code.
    const code = randomBytes(32).toString("hex");
    const codeHash = createHash("sha256").update(code).digest("hex");

    const expiresAt = Timestamp.fromMillis(
      Date.now() + EXPIRY_SECONDS * 1000
    );

    // Store only the hash, never the plaintext linking code.
    await adminDb.collection(COLLECTION).doc(codeHash).create({
      firebaseUID,
      createdAt: FieldValue.serverTimestamp(),
      expiresAt,
      usedAt: null,
    });

    return NextResponse.json({
      code,
      expiresInSeconds: EXPIRY_SECONDS,
    });
  } catch (error) {
    console.error("Telegram link-code error:", error);

    return NextResponse.json(
      { error: "Unable to generate Telegram linking code." },
      { status: 500 }
    );
  }
}
