import type { ScenarioDefinition } from "@/lib/scenarios/types";

const STORAGE_KEY = "pacey32-horseracing-scenarios-v1";

export interface SavedScenario extends ScenarioDefinition {
  savedAt: string;
  updatedAt: string;
}

function isBrowser(): boolean {
  return typeof window !== "undefined";
}

function isValidScenario(value: unknown): value is ScenarioDefinition {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }

  const scenario = value as Record<string, unknown>;

  return (
    typeof scenario.id === "string" &&
    scenario.id.length > 0 &&
    typeof scenario.name === "string" &&
    scenario.name.trim().length > 0 &&
    typeof scenario.startDate === "string" &&
    /^\d{4}-\d{2}-\d{2}$/.test(scenario.startDate) &&
    typeof scenario.endDate === "string" &&
    /^\d{4}-\d{2}-\d{2}$/.test(scenario.endDate) &&
    typeof scenario.stake === "number" &&
    Number.isFinite(scenario.stake) &&
    scenario.stake > 0 &&
    scenario.stake <= 100000 &&
    (scenario.betType === "win" ||
      scenario.betType === "each_way") &&
    (scenario.stakeAllocation === "per_horse" ||
      scenario.stakeAllocation === "per_race") &&
    (scenario.stakeMode === "total" ||
      scenario.stakeMode === "per_leg") &&
    typeof scenario.filters === "object" &&
    scenario.filters !== null &&
    !Array.isArray(scenario.filters)
  );
}

function readStorage(): SavedScenario[] {
  if (!isBrowser()) return [];

  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);

    if (!raw) return [];

    const parsed: unknown = JSON.parse(raw);

    if (!Array.isArray(parsed)) return [];

    return parsed.filter(isValidScenario).map((scenario) => {
      const saved = scenario as SavedScenario;

      return {
        ...saved,
        savedAt:
          typeof saved.savedAt === "string"
            ? saved.savedAt
            : new Date().toISOString(),
        updatedAt:
          typeof saved.updatedAt === "string"
            ? saved.updatedAt
            : new Date().toISOString(),
      };
    });
  } catch {
    return [];
  }
}

function writeStorage(scenarios: SavedScenario[]): void {
  if (!isBrowser()) return;

  window.localStorage.setItem(
    STORAGE_KEY,
    JSON.stringify(scenarios)
  );
}

export function getSavedScenarios(): SavedScenario[] {
  return readStorage().sort((a, b) =>
    b.updatedAt.localeCompare(a.updatedAt)
  );
}

export function saveScenario(
  definition: ScenarioDefinition
): SavedScenario {
  if (!isBrowser()) {
    throw new Error("Saved scenarios require a browser.");
  }

  if (!isValidScenario(definition)) {
    throw new Error("Invalid scenario definition.");
  }

  const existing = readStorage();
  const now = new Date().toISOString();

  const previous = existing.find(
    (scenario) => scenario.id === definition.id
  );

  const saved: SavedScenario = {
    ...definition,
    name: definition.name.trim(),
    savedAt: previous?.savedAt ?? now,
    updatedAt: now,
  };

  const updated = existing.filter(
    (scenario) => scenario.id !== definition.id
  );

  updated.push(saved);
  writeStorage(updated);

  return saved;
}

export function renameSavedScenario(
  id: string,
  name: string
): SavedScenario {
  const trimmedName = name.trim();

  if (!trimmedName) {
    throw new Error("Scenario name cannot be empty.");
  }

  if (trimmedName.length > 80) {
    throw new Error("Scenario name cannot exceed 80 characters.");
  }

  const existing = readStorage();

  const scenario = existing.find((item) => item.id === id);

  if (!scenario) {
    throw new Error("Saved scenario not found.");
  }

  const updatedScenario: SavedScenario = {
    ...scenario,
    name: trimmedName,
    updatedAt: new Date().toISOString(),
  };

  writeStorage(
    existing.map((item) =>
      item.id === id ? updatedScenario : item
    )
  );

  return updatedScenario;
}

export function deleteSavedScenario(id: string): void {
  writeStorage(
    readStorage().filter((scenario) => scenario.id !== id)
  );
}

export function exportSavedScenarios(): string {
  return JSON.stringify(
    {
      format: "pacey32-horseracing-scenarios",
      version: 1,
      exportedAt: new Date().toISOString(),
      scenarios: getSavedScenarios(),
    },
    null,
    2
  );
}

export function importSavedScenarios(
  json: string
): {
  imported: number;
  skipped: number;
} {
  if (!isBrowser()) {
    throw new Error("Import requires a browser.");
  }

  let parsed: unknown;

  try {
    parsed = JSON.parse(json);
  } catch {
    throw new Error("Invalid JSON file.");
  }

  if (
    !parsed ||
    typeof parsed !== "object" ||
    Array.isArray(parsed)
  ) {
    throw new Error("Invalid scenarios file.");
  }

  const file = parsed as Record<string, unknown>;

  if (
    file.format !== "pacey32-horseracing-scenarios" ||
    file.version !== 1 ||
    !Array.isArray(file.scenarios)
  ) {
    throw new Error("Unsupported scenarios file format.");
  }

  const existing = readStorage();
  const existingIds = new Set(
    existing.map((scenario) => scenario.id)
  );

  const now = new Date().toISOString();

  let imported = 0;
  let skipped = 0;

  for (const item of file.scenarios) {
    if (!isValidScenario(item)) {
      skipped++;
      continue;
    }

    // Imported scenarios receive new IDs to avoid
    // overwriting existing saved scenarios.
    const saved: SavedScenario = {
      ...item,
      id: crypto.randomUUID(),
      savedAt: now,
      updatedAt: now,
    };

    if (existingIds.has(saved.id)) {
      skipped++;
      continue;
    }

    existing.push(saved);
    existingIds.add(saved.id);
    imported++;
  }

  writeStorage(existing);

  return { imported, skipped };
}