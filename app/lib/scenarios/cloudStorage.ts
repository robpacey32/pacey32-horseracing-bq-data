
import {
  collection,
  deleteDoc,
  doc,
  getDoc,
  getDocs,
  runTransaction,
  setDoc,
  updateDoc,
} from "firebase/firestore";

import { db } from "@/lib/firebase";
import type { ScenarioDefinition } from "@/lib/scenarios/types";
import type { SavedScenario } from "@/lib/scenarios/storage";

function scenariosCollection(uid: string) {
  return collection(db, "users", uid, "scenarios");
}

function scenarioDocument(uid: string, id: string) {
  return doc(db, "users", uid, "scenarios", id);
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
  const reference = scenarioDocument(uid, definition.id);
  const previous = await getDoc(reference);
  const now = new Date().toISOString();

  const saved: SavedScenario = {
    ...definition,
    name: definition.name.trim(),
    savedAt: previous.exists()
      ? (previous.data().savedAt as string) ?? now
      : now,
    updatedAt: now,
  };

  // Firestore does not accept undefined values.
  const clean = JSON.parse(
    JSON.stringify(saved)
  ) as SavedScenario;

  await setDoc(reference, clean);
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

  await updateDoc(scenarioDocument(uid, id), {
    name: trimmed,
    updatedAt: new Date().toISOString(),
  });
}

export async function deleteCloudScenario(
  uid: string,
  id: string
): Promise<void> {
  await deleteDoc(scenarioDocument(uid, id));
}


export async function migrateLocalScenarios(
  uid: string,
  localScenarios: SavedScenario[]
): Promise<{ imported: number; skipped: number }> {
  let imported = 0;
  let skipped = 0;

  for (const scenario of localScenarios) {
    const reference = scenarioDocument(uid, scenario.id);

    const clean = JSON.parse(
      JSON.stringify(scenario)
    ) as SavedScenario;

    const wasImported = await runTransaction(
      db,
      async (transaction) => {
        const existing = await transaction.get(reference);

        if (existing.exists()) {
          return false;
        }

        transaction.set(reference, clean);
        return true;
      }
    );

    if (wasImported) {
      imported++;
    } else {
      skipped++;
    }
  }

  return { imported, skipped };
}

