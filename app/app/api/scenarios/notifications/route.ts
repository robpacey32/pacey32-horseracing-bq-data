
import { NextRequest, NextResponse } from "next/server";
import { adminAuth, adminDb } from "@/lib/firebaseAdmin";

const MAX_ENABLED = 10;

export async function POST(request: NextRequest) {
  try {
    const authorization = request.headers.get("authorization");

    if (!authorization?.startsWith("Bearer ")) {
      return NextResponse.json(
        { error: "Authentication required." },
        { status: 401 }
      );
    }

    const token = authorization.slice(7);
    const decoded = await adminAuth.verifyIdToken(token, true);

    const body: unknown = await request.json();

    if (!body || typeof body !== "object" || Array.isArray(body)) {
      return NextResponse.json(
        { error: "Invalid request." },
        { status: 400 }
      );
    }

    const { scenarioId, enabled } = body as Record<string, unknown>;

    if (
      typeof scenarioId !== "string" ||
      !/^[a-zA-Z0-9_-]{1,100}$/.test(scenarioId) ||
      typeof enabled !== "boolean"
    ) {
      return NextResponse.json(
        { error: "Invalid notification settings." },
        { status: 400 }
      );
    }

    const userRef = adminDb.collection("users").doc(decoded.uid);
    const scenariosRef = userRef.collection("scenarios");
    const scenarioRef = scenariosRef.doc(scenarioId);

    const result = await adminDb.runTransaction(async (transaction) => {
      // Shared document coordinates concurrent updates for this user.
      const userSnapshot = await transaction.get(userRef);

      // Read the complete scenario collection before writing.
      const scenarios = await transaction.get(scenariosRef);

      const target = scenarios.docs.find(
        (doc) => doc.id === scenarioId
      );

      if (!target) {
        return { status: 404, error: "Scenario not found." };
      }

      const currentlyEnabled = scenarios.docs.filter(
        (doc) => doc.get("notificationsEnabled") === true
      ).length;

      const alreadyEnabled =
        target.get("notificationsEnabled") === true;

      if (enabled && !alreadyEnabled && currentlyEnabled >= MAX_ENABLED) {
        return {
          status: 409,
          error: "You can enable notifications for up to 10 scenarios.",
        };
      }

      const enabledCount =
        currentlyEnabled +
        (enabled && !alreadyEnabled ? 1 : 0) -
        (!enabled && alreadyEnabled ? 1 : 0);

      transaction.update(scenarioRef, {
        notificationsEnabled: enabled,
        updatedAt: new Date().toISOString(),
      });

      // Every notification change writes the same parent document.
      // This creates a transaction conflict if two updates race.
      transaction.set(
        userRef,
        {
          notificationSettingsVersion:
            (userSnapshot.get("notificationSettingsVersion") ?? 0) + 1,
        },
        { merge: true }
      );

      return { status: 200, enabledCount };
    });

    if (result.status !== 200) {
      return NextResponse.json(
        { error: result.error },
        { status: result.status }
      );
    }

    return NextResponse.json({
      success: true,
      enabledCount: result.enabledCount,
      limit: MAX_ENABLED,
    });
  } catch (error) {
    console.error("Scenario notification error:", error);

    return NextResponse.json(
      { error: "Unable to update notification settings." },
      { status: 500 }
    );
  }
}
