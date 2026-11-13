
import {
  collection,
  getDocs,
} from "firebase/firestore";

import { db, auth } from "@/lib/firebase";
import type { ScenarioDefinition } from "@/lib/scenarios/types";
import type { SavedScenario } from "@/lib/scenarios/storage";

function scenariosCollection(uid: string) {
  return collection(db, "users", uid, "scenarios");
}

async function scenarioApi(
  method: "POST" | "DELETE",
  body: Record<string, unknown>,
  uid: string
): Promise<{ success: boolean; saved?: boolean }> {
  const user = auth.currentUser;

  if (!user || user.uid !== uid) {
    throw new Error("Please sign in to manage scenarios.");
  }

  const token = await user.getIdToken();

  const response = await fetch("/api/scenarios", {
    method,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(body),
  });

  const result = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(
      result?.error ?? "Unable to manage scenario."
    );
  }

  return result;
}

export async function getCloudScenarios(
  uid: string
): Promise<SavedScenario[]> {
  const snapshot = await getDocs(scenariosCollection(uid));

  return snapshot.docs
    .map((item) => ({
      ...item.data(),
      id: item.id,
    }) as SavedScenario)
    .sort((a, b) =>
      (b.updatedAt ?? "").localeCompare(a.updatedAt ?? "")
    );
}

export async function saveCloudScenario(
  uid: string,
  definition: ScenarioDefinition
): Promise<void> {
  await scenarioApi("POST", { ...definition }, uid);
}

export async function renameCloudScenario(
  uid: string,
  id: string,
  name: string
): Promise<void> {
  const trimmed = name.trim();

  if (!trimmed || trimmed.length > 80) {
    throw new Error("Name must contain 1–80 characters.");
  }

  await scenarioApi(
    "POST",
    {
      scenarioId: id,
      name: trimmed,
      action: "rename",
    },
    uid
  );
}

export async function deleteCloudScenario(
  uid: string,
  id: string
): Promise<void> {
  await scenarioApi("DELETE", { scenarioId: id }, uid);
}

export async function migrateLocalScenarios(
  uid: string,
  localScenarios: SavedScenario[]
): Promise<{ imported: number; skipped: number }> {
  let imported = 0;
  let skipped = 0;

  for (const scenario of localScenarios) {
    const result = await scenarioApi(
      "POST",
      { ...scenario, importOnly: true },
      uid
    );

    if (result.saved) {
      imported++;
    } else {
      skipped++;
    }
  }

  return { imported, skipped };
}

export async function updateScenarioNotifications(
  scenarioId: string,
  enabled: boolean
): Promise<void> {
  const user = auth.currentUser;

  if (!user) {
    throw new Error("Please sign in to manage notifications.");
  }

  const token = await user.getIdToken();

  const response = await fetch("/api/scenarios/notifications", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({ scenarioId, enabled }),
  });

  if (!response.ok) {
    const result = await response.json().catch(() => null);

    throw new Error(
      result?.error ?? "Unable to update notification settings."
    );
  }
}
