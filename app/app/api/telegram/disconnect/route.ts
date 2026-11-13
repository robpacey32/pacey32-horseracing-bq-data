
import { NextRequest, NextResponse } from "next/server";
import { adminAuth, adminDb } from "@/lib/firebaseAdmin";
import { getBigQueryClient } from "@/lib/bigquery";

const LINKS_COLLECTION = "telegramAccountLinks";
const REVERSE_COLLECTION = "telegramUserLinks";

export async function POST(request: NextRequest) {
  try {
    const authorization = request.headers.get(
      "authorization"
    );

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

    const firebaseUID = decoded.uid;

    const result = await adminDb.runTransaction(
      async (transaction) => {
        const firebaseRef = adminDb
          .collection(LINKS_COLLECTION)
          .doc(firebaseUID);

        const firebaseSnapshot = await transaction.get(
          firebaseRef
        );

        if (!firebaseSnapshot.exists) {
          return {
            disconnected: true,
            alreadyDisconnected: true,
            telegramUserID: null as string | null,
          };
        }

        const firebaseData =
          firebaseSnapshot.data() || {};

        const telegramUserID =
          firebaseData.telegramUserID;

        if (
          typeof telegramUserID !== "string" ||
          !telegramUserID
        ) {
          throw new Error(
            "Telegram account mapping is incomplete."
          );
        }

        const telegramRef = adminDb
          .collection(REVERSE_COLLECTION)
          .doc(telegramUserID);

        const telegramSnapshot = await transaction.get(
          telegramRef
        );

        // Ensure that the reverse link does not belong
        // to another Firebase account.
        if (telegramSnapshot.exists) {
          const telegramData =
            telegramSnapshot.data() || {};

          if (
            telegramData.firebaseUID !== firebaseUID
          ) {
            throw new Error(
              "Telegram account mapping is inconsistent."
            );
          }
        }

        // All reads are completed before writes.
        // Both records are removed atomically.

        transaction.delete(firebaseRef);

        if (telegramSnapshot.exists) {
          transaction.delete(telegramRef);
        }

        return {
          disconnected: true,
          alreadyDisconnected: false,
          telegramUserID,
        };
      }
    );

    // No Firestore connection existed.
    if (result.alreadyDisconnected) {
      return NextResponse.json({
        disconnected: true,
        alreadyDisconnected: true,
      });
    }

    // Remove only this user's mapping from the
    // BigQuery mirror. Do not modify TelegramUsers,
    // which controls the existing broadcasts.

    try {
      const bigquery = getBigQueryClient();

      await bigquery.query({
        query: `
          DELETE FROM
            \`horseracing-pacey32-github.bettingalerts.TelegramAccountLinks\`
          WHERE FirebaseUID = @firebaseUID
            AND TelegramUserID = @telegramUserID
        `,
        params: {
          firebaseUID,
          telegramUserID: result.telegramUserID,
        },
      });
    } catch (syncError) {
      // The Firestore disconnect has succeeded.
      // Report that the BigQuery mirror needs repair.
      console.error(
        "Telegram disconnect succeeded, but BigQuery sync failed:",
        syncError
      );

      return NextResponse.json({
        disconnected: true,
        syncPending: true,
        message:
          "Telegram disconnected. The analytics mapping still needs synchronisation.",
      });
    }

    return NextResponse.json({
      disconnected: true,
      syncPending: false,
      message: "Telegram disconnected successfully.",
    });
  } catch (error) {
    console.error(
      "Telegram disconnect error:",
      error
    );

    return NextResponse.json(
      {
        error:
          "Unable to disconnect Telegram. Please try again.",
      },
      { status: 500 }
    );
  }
}
