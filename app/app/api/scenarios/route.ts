
import { NextRequest, NextResponse } from "next/server";
import { adminAuth, adminDb } from "@/lib/firebaseAdmin";
import type {
  ScenarioDefinition,
  ScenarioFilters,
} from "@/lib/scenarios/types";

const ID_PATTERN = /^[a-zA-Z0-9_-]{1,100}$/;
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

const FILTER_KEYS = [
  "minOdds", "maxOdds",
  "minPreRaceRank", "maxPreRaceRank",
  "favouriteStatus",
  "raceType", "raceTypes",
  "course", "courses",
  "raceClass", "raceClasses",
  "going", "goings",
  "surface", "surfaces",
  "minDistance", "maxDistance",
  "handicap",
  "courseWinner", "distanceWinner",
  "courseDistanceWinner", "beatenFavourite",
  "wonInLast3", "win2InLast6",
  "eachWayEligible",
] as const;

const NUMBER_FILTERS = new Set<string>([
  "minOdds", "maxOdds",
  "minPreRaceRank", "maxPreRaceRank",
  "minDistance", "maxDistance",
]);

const BOOLEAN_FILTERS = new Set<string>([
  "handicap",
  "courseWinner", "distanceWinner",
  "courseDistanceWinner", "beatenFavourite",
  "wonInLast3", "win2InLast6",
  "eachWayEligible",
]);

const STRING_FILTERS = new Set<string>([
  "raceType", "course", "raceClass",
  "going", "surface",
]);

const ARRAY_FILTERS = new Set<string>([
  "raceTypes", "courses", "raceClasses",
  "goings", "surfaces",
]);

function isRecord(
  value: unknown
): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value)
  );
}

function isValidDate(value: unknown): value is string {
  if (typeof value !== "string" || !DATE_PATTERN.test(value)) {
    return false;
  }

  const date = new Date(`${value}T00:00:00Z`);

  return (
    !Number.isNaN(date.getTime()) &&
    date.toISOString().slice(0, 10) === value
  );
}

function isValidFilters(
  value: unknown
): value is ScenarioFilters {
  if (!isRecord(value)) return false;

  for (const [key, filterValue] of Object.entries(value)) {
    if (!(FILTER_KEYS as readonly string[]).includes(key)) {
      return false;
    }

    if (NUMBER_FILTERS.has(key)) {
      if (
        typeof filterValue !== "number" ||
        !Number.isFinite(filterValue) ||
        filterValue < 0
      ) {
        return false;
      }
    } else if (BOOLEAN_FILTERS.has(key)) {
      if (typeof filterValue !== "boolean") return false;
    } else if (STRING_FILTERS.has(key)) {
      if (typeof filterValue !== "string") return false;
    } else if (ARRAY_FILTERS.has(key)) {
      if (
        !Array.isArray(filterValue) ||
        !filterValue.every(
          (item) => typeof item === "string"
        )
      ) {
        return false;
      }
    } else if (key === "favouriteStatus") {
      if (
        filterValue !== "favourite" &&
        filterValue !== "not_favourite"
      ) {
        return false;
      }
    }
  }

  return true;
}

function parseScenario(body: unknown): {
  definition: ScenarioDefinition;
  importOnly: boolean;
} | null {
  if (!isRecord(body)) return null;

  const {
    id,
    name,
    startDate,
    endDate,
    stake,
    betType,
    stakeAllocation,
    stakeMode,
    filters,
    importOnly,
  } = body;

  if (
    typeof id !== "string" ||
    !ID_PATTERN.test(id) ||
    typeof name !== "string" ||
    !name.trim() ||
    name.trim().length > 80 ||
    !isValidDate(startDate) ||
    !isValidDate(endDate) ||
    startDate > endDate ||
    typeof stake !== "number" ||
    !Number.isFinite(stake) ||
    stake <= 0 ||
    (betType !== "win" && betType !== "each_way") ||
    (stakeAllocation !== "per_horse" &&
      stakeAllocation !== "per_race") ||
    (stakeMode !== "total" && stakeMode !== "per_leg") ||
    !isValidFilters(filters) ||
    (importOnly !== undefined &&
      typeof importOnly !== "boolean")
  ) {
    return null;
  }

  return {
    definition: {
      id,
      name: name.trim(),
      startDate,
      endDate,
      stake,
      betType,
      stakeAllocation,
      stakeMode,
      filters,
    },
    importOnly: importOnly === true,
  };
}

async function authenticate(request: NextRequest) {
  const authorization = request.headers.get("authorization");

  if (!authorization?.startsWith("Bearer ")) {
    return null;
  }

  try {
    return await adminAuth.verifyIdToken(
      authorization.slice(7),
      true
    );
  } catch {
    return null;
  }
}

function nextVersion(value: unknown): number {
  return typeof value === "number" &&
    Number.isFinite(value)
    ? value + 1
    : 1;
}

function normalizedName(name: string): string {
  return name.trim().toLowerCase();
}

export async function POST(request: NextRequest) {
  try {
    const user = await authenticate(request);

    if (!user) {
      return NextResponse.json(
        { error: "Authentication required." },
        { status: 401 }
      );
    }

    const body: unknown = await request.json();

    if (!isRecord(body)) {
      return NextResponse.json(
        { error: "Invalid request." },
        { status: 400 }
      );
    }

    const isRename = body.action === "rename";

    let scenarioId: string;
    let scenarioName: string;
    let definition: ScenarioDefinition | null = null;
    let importOnly = false;

    if (isRename) {
      if (
        typeof body.scenarioId !== "string" ||
        !ID_PATTERN.test(body.scenarioId) ||
        typeof body.name !== "string" ||
        !body.name.trim() ||
        body.name.trim().length > 80
      ) {
        return NextResponse.json(
          { error: "Invalid scenario name or ID." },
          { status: 400 }
        );
      }

      scenarioId = body.scenarioId;
      scenarioName = body.name.trim();
    } else {
      const parsed = parseScenario(body);

      if (!parsed) {
        return NextResponse.json(
          { error: "Invalid scenario definition." },
          { status: 400 }
        );
      }

      definition = parsed.definition;
      importOnly = parsed.importOnly;
      scenarioId = definition.id;
      scenarioName = definition.name;
    }

    const userRef = adminDb
      .collection("users")
      .doc(user.uid);

    const scenariosRef = userRef.collection("scenarios");
    const scenarioRef = scenariosRef.doc(scenarioId);

    const result = await adminDb.runTransaction(
      async (transaction) => {
        // Read all documents before performing writes.
        const userSnapshot = await transaction.get(userRef);
        const scenariosSnapshot =
          await transaction.get(scenariosRef);

        const existing = scenariosSnapshot.docs.find(
          (doc) => doc.id === scenarioId
        );

        if (isRename && !existing) {
          return {
            status: 404,
            error: "Scenario not found.",
            saved: false,
          };
        }

        // Migration must not overwrite an existing scenario.
        if (importOnly && existing) {
          return {
            status: 200,
            saved: false,
          };
        }

        // Prevent duplicate names belonging to the same user.
        const duplicate = scenariosSnapshot.docs.some(
          (doc) => {
            if (doc.id === scenarioId) return false;

            const existingName = doc.get("name");

            return (
              typeof existingName === "string" &&
              normalizedName(existingName) ===
                normalizedName(scenarioName)
            );
          }
        );

        if (duplicate) {
          return {
            status: 409,
            error:
              `A scenario named "${scenarioName}" ` +
              "already exists.",
            saved: false,
          };
        }

        const now = new Date().toISOString();

        if (isRename) {
          transaction.update(scenarioRef, {
            name: scenarioName,
            updatedAt: now,
          });
        } else if (definition) {
          if (existing) {
            // Preserve notification fields on existing scenarios.
            transaction.update(scenarioRef, {
              ...definition,
              updatedAt: now,
            });
          } else {
            transaction.set(scenarioRef, {
              ...definition,
              savedAt: now,
              updatedAt: now,
            });
          }
        }

        // Coordinate with notification updates and deletions.
        transaction.set(
          userRef,
          {
            notificationSettingsVersion: nextVersion(
              userSnapshot.get("notificationSettingsVersion")
            ),
          },
          { merge: true }
        );

        return {
          status: 200,
          saved: true,
        };
      }
    );

    if (result.status !== 200) {
      return NextResponse.json(
        { error: result.error },
        { status: result.status }
      );
    }

    return NextResponse.json({
      success: true,
      saved: result.saved,
    });
  } catch (error) {
    console.error("Scenario save/rename error:", error);

    return NextResponse.json(
      { error: "Unable to save scenario." },
      { status: 500 }
    );
  }
}

export async function DELETE(request: NextRequest) {
  try {
    const user = await authenticate(request);

    if (!user) {
      return NextResponse.json(
        { error: "Authentication required." },
        { status: 401 }
      );
    }

    const body: unknown = await request.json();

    if (!isRecord(body)) {
      return NextResponse.json(
        { error: "Invalid request." },
        { status: 400 }
      );
    }

    const scenarioId = body.scenarioId;

    if (
      typeof scenarioId !== "string" ||
      !ID_PATTERN.test(scenarioId)
    ) {
      return NextResponse.json(
        { error: "Invalid scenario ID." },
        { status: 400 }
      );
    }

    const userRef = adminDb
      .collection("users")
      .doc(user.uid);

    const scenarioRef = userRef
      .collection("scenarios")
      .doc(scenarioId);

    const deleted = await adminDb.runTransaction(
      async (transaction) => {
        const userSnapshot = await transaction.get(userRef);
        const existing = await transaction.get(scenarioRef);

        if (!existing.exists) {
          return false;
        }

        transaction.delete(scenarioRef);

        transaction.set(
          userRef,
          {
            notificationSettingsVersion: nextVersion(
              userSnapshot.get("notificationSettingsVersion")
            ),
          },
          { merge: true }
        );

        return true;
      }
    );

    return NextResponse.json({
      success: true,
      deleted,
    });
  } catch (error) {
    console.error("Scenario delete error:", error);

    return NextResponse.json(
      { error: "Unable to delete scenario." },
      { status: 500 }
    );
  }
}
