
"use client";

import { useEffect, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";
import Link from "next/link";

import { auth } from "@/lib/firebase";
import type {
  ScenarioDefinition,
  ScenarioFilters,
} from "@/lib/scenarios/types";
import {
  getSavedScenarios,
  deleteSavedScenario,
  type SavedScenario,
} from "@/lib/scenarios/storage";
import {
  getCloudScenarios,
  renameCloudScenario,
  deleteCloudScenario,
  migrateLocalScenarios,
} from "@/lib/scenarios/cloudStorage";

interface Props {
  onLoadScenario: (scenario: ScenarioDefinition) => void;

  // Legacy single-selection interface.
  selectedId?: string | null;
  onSelect?: (scenario: ScenarioDefinition) => void;

  // New multi-selection interface.
  selectedIds?: string[];
  onToggle?: (scenario: ScenarioDefinition) => void;
}

function formatDate(value: string): string {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) return value;

  return date.toLocaleDateString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  });
}

export function formatScenarioFilters(
  filters: ScenarioFilters
): string {
  const parts: string[] = [];

  function range(
    label: string,
    min?: number,
    max?: number
  ) {
    if (min !== undefined && max !== undefined) {
      parts.push(`${label}: ${min}–${max}`);
    } else if (min !== undefined) {
      parts.push(`${label}: at least ${min}`);
    } else if (max !== undefined) {
      parts.push(`${label}: up to ${max}`);
    }
  }

  range("Decimal odds", filters.minOdds, filters.maxOdds);
  range(
    "Odds rank",
    filters.minPreRaceRank,
    filters.maxPreRaceRank
  );
  range(
    "Distance (f)",
    filters.minDistance,
    filters.maxDistance
  );

  if (filters.favouriteStatus === "favourite") {
    parts.push("Favourites only");
  } else if (
    filters.favouriteStatus === "not_favourite"
  ) {
    parts.push("Exclude favourites");
  }

  const collections: {
    label: string;
    values?: string[];
    single?: string;
  }[] = [
    {
      label: "Race type",
      values: filters.raceTypes,
      single: filters.raceType,
    },
    {
      label: "Course",
      values: filters.courses,
      single: filters.course,
    },
    {
      label: "Class",
      values: filters.raceClasses,
      single: filters.raceClass,
    },
    {
      label: "Going",
      values: filters.goings,
      single: filters.going,
    },
    {
      label: "Surface",
      values: filters.surfaces,
      single: filters.surface,
    },
  ];

  for (const collection of collections) {
    const values = collection.values?.length
      ? collection.values
      : collection.single
        ? [collection.single]
        : [];

    if (values.length) {
      parts.push(
        `${collection.label}: ${values.join(", ")}`
      );
    }
  }

  const booleanLabels: {
    key: keyof ScenarioFilters;
    label: string;
  }[] = [
    { key: "handicap", label: "Handicap" },
    { key: "courseWinner", label: "C" },
    { key: "distanceWinner", label: "D" },
    { key: "courseDistanceWinner", label: "CD" },
    { key: "beatenFavourite", label: "BF" },
    { key: "wonInLast3", label: "Won in last 3" },
    { key: "win2InLast6", label: "Won twice in last 6" },
    { key: "eachWayEligible", label: "Each-way eligible" },
  ];

  for (const { key, label } of booleanLabels) {
    const value = filters[key];

    if (value === true) parts.push(label);
    if (value === false) parts.push(`Exclude ${label}`);
  }

  return parts.length
    ? parts.join(" · ")
    : "All horses — no additional filters";
}

export default function SavedScenarios({
  onLoadScenario,
  selectedId = null,
  onSelect,
  selectedIds,
  onToggle,
}: Props) {
  const [user, setUser] = useState<User | null>(null);
  const [checkingAuth, setCheckingAuth] = useState(true);
  const [savedScenarios, setSavedScenarios] =
    useState<SavedScenario[]>([]);
  const [localIds, setLocalIds] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [localSelectedIds, setLocalSelectedIds] =
    useState<string[]>([]);

  const activeIds =
    selectedIds ??
    (onSelect
      ? selectedId
        ? [selectedId]
        : []
      : localSelectedIds);

  useEffect(() => {
    return onAuthStateChanged(auth, (currentUser) => {
      setUser(currentUser);
      setCheckingAuth(false);
      setMessage(null);
      setError(null);
    });
  }, []);

  useEffect(() => {
    let cancelled = false;

    setSavedScenarios([]);
    setLoading(Boolean(user));

    if (!user) {
      return () => {
        cancelled = true;
      };
    }

    async function load() {
      try {
        const scenarios = await getCloudScenarios(user!.uid);

        if (!cancelled) {
          setSavedScenarios(scenarios);
        }
      } catch (err) {
        if (!cancelled) {
          setError(
            err instanceof Error
              ? err.message
              : "Unable to load cloud scenarios."
          );
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    void load();

    return () => {
      cancelled = true;
    };
  }, [user]);

  useEffect(() => {
    refreshLocalIds();
  }, []);

  useEffect(() => {
    if (!user) return;

    const handleScenarioSaved = () => {
      void refresh(user.uid).catch((err: unknown) => {
        showError(err, "Unable to refresh saved scenarios.");
      });
    };

    window.addEventListener("pacey32:scenario-saved", handleScenarioSaved);
    return () => {
      window.removeEventListener("pacey32:scenario-saved", handleScenarioSaved);
    };
  }, [user]);

  function refreshLocalIds() {
    try {
      setLocalIds(
        getSavedScenarios().map((scenario) => scenario.id)
      );
    } catch {
      setLocalIds([]);
    }
  }

  async function refresh(uid: string) {
    const scenarios = await getCloudScenarios(uid);

    if (auth.currentUser?.uid === uid) {
      setSavedScenarios(scenarios);
    }
  }

  function showError(err: unknown, fallback: string) {
    setMessage(null);
    setError(
      err instanceof Error ? err.message : fallback
    );
  }

  function toggleScenario(scenario: SavedScenario) {
    setMessage(null);
    setError(null);

    if (onToggle) {
      onToggle(scenario);
      return;
    }

    if (onSelect) {
      onSelect(scenario);
      return;
    }

    setLocalSelectedIds((previous) =>
      previous.includes(scenario.id)
        ? previous.filter((id) => id !== scenario.id)
        : [...previous, scenario.id]
    );
  }

  function handleLoad(scenario: SavedScenario) {
    onLoadScenario(scenario);
    setMessage(
      `Loaded "${scenario.name}" into Scenario Builder.`
    );
    setError(null);
  }

  async function handleRename(scenario: SavedScenario) {
    if (!user || busy) return;

    const name = window.prompt(
      "Rename saved scenario:",
      scenario.name
    );

    if (name === null || !name.trim()) return;

    setBusy(true);
    setError(null);
    setMessage(null);

    try {
      await renameCloudScenario(
        user.uid,
        scenario.id,
        name.trim()
      );

      await refresh(user.uid);
      setMessage("Scenario renamed.");
    } catch (err) {
      showError(err, "Unable to rename scenario.");
    } finally {
      setBusy(false);
    }
  }

  async function handleDelete(scenario: SavedScenario) {
    if (!user || busy) return;

    const confirmed = window.confirm(
      `Delete saved scenario "${scenario.name}"?`
    );

    if (!confirmed) return;

    setBusy(true);
    setError(null);
    setMessage(null);

    try {
      await deleteCloudScenario(user.uid, scenario.id);
      await refresh(user.uid);

      setLocalSelectedIds((previous) =>
        previous.filter((id) => id !== scenario.id)
      );

      setMessage("Saved scenario deleted.");
    } catch (err) {
      showError(err, "Unable to delete scenario.");
    } finally {
      setBusy(false);
    }
  }

  async function handleMigration() {
    if (!user || busy) return;

    const local = getSavedScenarios();

    const cloudIds = new Set(
      savedScenarios.map((scenario) => scenario.id)
    );

    const unmigrated = local.filter(
      (scenario) => !cloudIds.has(scenario.id)
    );

    if (!unmigrated.length) {
      setMessage(
        "All browser-saved scenarios are already in your account."
      );
      return;
    }

    const confirmed = window.confirm(
      `Import ${unmigrated.length} browser-saved scenario(s) ` +
        "into your account? Existing cloud scenarios will " +
        "be preserved. Browser copies will remain."
    );

    if (!confirmed) return;

    setBusy(true);
    setError(null);
    setMessage(null);

    try {
      const result = await migrateLocalScenarios(
        user.uid,
        unmigrated
      );

      await refresh(user.uid);

      setMessage(
        `Migrated ${result.imported} scenario(s). ` +
          `${result.skipped} skipped. Browser copies preserved.`
      );
    } catch (err) {
      showError(err, "Unable to migrate local scenarios.");
    } finally {
      refreshLocalIds();
      setBusy(false);
    }
  }

  function handleClearBrowserScenarios() {
    if (busy) return;

    const cloudIds = new Set(
      savedScenarios.map((scenario) => scenario.id)
    );

    let unmigrated: SavedScenario[];

    try {
      unmigrated = getSavedScenarios().filter(
        (scenario) => !cloudIds.has(scenario.id)
      );
    } catch (err) {
      showError(
        err,
        "Unable to read browser-saved scenarios."
      );
      return;
    }

    if (!unmigrated.length) {
      refreshLocalIds();
      setMessage("No unmigrated browser scenarios to clear.");
      return;
    }

    const confirmed = window.confirm(
      `Permanently remove ${unmigrated.length} ` +
        "unmigrated scenario(s) from this browser?\n\n" +
        "These scenarios may not exist in your cloud account. " +
        "This cannot be undone. Cloud scenarios will not be deleted."
    );

    if (!confirmed) return;

    try {
      for (const scenario of unmigrated) {
        deleteSavedScenario(scenario.id);
      }

      refreshLocalIds();
      setError(null);
      setMessage(
        `Removed ${unmigrated.length} browser-saved scenario(s).`
      );
    } catch (err) {
      refreshLocalIds();
      showError(
        err,
        "Unable to clear browser-saved scenarios."
      );
    }
  }

  const cloudIds = new Set(
    savedScenarios.map((scenario) => scenario.id)
  );

  const unmigratedCount = localIds.filter(
    (id) => !cloudIds.has(id)
  ).length;

  return (
    <section className="rounded-xl border border-[#ded8c9] bg-white p-4 md:p-5">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h3 className="font-serif text-xl text-[#123d2d]">
            Your saved scenarios
          </h3>

          <p className="mt-1 text-xs text-gray-500">
            Tick scenarios to compare performance, or load
            one into the builder to edit it.
          </p>
        </div>


      </div>

      {message && (
        <p className="mb-3 text-sm text-green-700">
          {message}
        </p>
      )}

      {error && (
        <p
          role="alert"
          className="mb-3 text-sm text-red-700"
        >
          {error}
        </p>
      )}

      {checkingAuth ? (
        <p className="text-sm text-gray-500">
          Checking account…
        </p>
      ) : !user ? (
        <div className="rounded-lg bg-gray-50 p-4 text-sm text-gray-600">
          Sign in to save and access your scenarios across
          devices.{" "}
          <Link
            href="/account"
            className="font-semibold text-green-800 underline"
          >
            Go to your account
          </Link>
          .
        </div>
      ) : (
        <>
          {!loading && unmigratedCount > 0 && (
            <div className="mb-4 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-gray-200 bg-gray-50 p-3">
              <p className="text-sm text-gray-600">
                {unmigratedCount} browser-saved scenario(s)
                not yet in your account.
              </p>

              <div className="flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() => void handleMigration()}
                  disabled={busy}
                  className="rounded-md border border-gray-300 bg-white px-3 py-1.5 text-xs font-medium disabled:opacity-40"
                >
                  Migrate to Account
                </button>

                <button
                  type="button"
                  onClick={handleClearBrowserScenarios}
                  disabled={busy}
                  className="rounded-md border border-red-200 bg-white px-3 py-1.5 text-xs font-medium text-red-700 disabled:opacity-40"
                >
                  Clear Browser Copies
                </button>
              </div>
            </div>
          )}

          {loading ? (
            <p className="text-sm text-gray-500">
              Loading cloud scenarios…
            </p>
          ) : savedScenarios.length === 0 ? (
            <p className="rounded-lg bg-gray-50 px-4 py-5 text-sm text-gray-500">
              No saved scenarios yet. Run a scenario in the
              builder, then click Save Scenario.
            </p>
          ) : (
            <div className="divide-y divide-[#e8e2d6] overflow-hidden rounded-lg border border-[#ded8c9]">
              {savedScenarios.map((scenario) => {
                const selected = activeIds.includes(
                  scenario.id
                );

                return (
                  <div
                    key={scenario.id}
                    className={`px-3 py-3 ${
                      selected
                        ? "bg-[#edf4ed]"
                        : "bg-white"
                    }`}
                  >
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <label className="flex min-w-0 flex-1 cursor-pointer items-start gap-3">
                        <input
                          type="checkbox"
                          checked={selected}
                          onChange={() =>
                            toggleScenario(scenario)
                          }
                          className="mt-1 accent-[#123d2d]"
                          aria-label={`Compare ${scenario.name}`}
                        />

                        <span className="min-w-0">
                          <span className="block font-semibold text-[#123d2d]">
                            {scenario.name}
                          </span>

                          <span className="mt-0.5 block text-xs text-gray-500">
                            {scenario.startDate} to{" "}
                            {scenario.endDate}
                            {" · "}
                            Saved {formatDate(scenario.savedAt)}
                            {" · "}
                            £{scenario.stake.toFixed(2)}
                            {" · "}
                            {scenario.betType === "each_way"
                              ? "Each-way"
                              : "Win"}
                          </span>

                          <span className="mt-1 block text-xs leading-relaxed text-gray-600">
                            {formatScenarioFilters(
                              scenario.filters
                            )}
                          </span>

                          <span className="mt-1 block text-xs text-gray-500">
                            {scenario.stakeAllocation ===
                            "per_race"
                              ? "Stake shared per race"
                              : "Stake per horse"}
                            {" · "}
                            {scenario.stakeMode === "per_leg"
                              ? "Stake per leg"
                              : "Total stake"}
                          </span>
                        </span>
                      </label>

                      <div className="flex flex-wrap gap-1.5">
                        <button
                          type="button"
                          onClick={() =>
                            handleLoad(scenario)
                          }
                          disabled={busy}
                          className="rounded-md border border-gray-300 px-2.5 py-1.5 text-xs disabled:opacity-40"
                        >
                          Edit in Builder
                        </button>

                        <button
                          type="button"
                          onClick={() =>
                            void handleRename(scenario)
                          }
                          disabled={busy}
                          className="rounded-md border border-gray-300 px-2.5 py-1.5 text-xs disabled:opacity-40"
                        >
                          Rename
                        </button>

                        <button
                          type="button"
                          onClick={() =>
                            void handleDelete(scenario)
                          }
                          disabled={busy}
                          className="rounded-md border border-red-200 px-2.5 py-1.5 text-xs text-red-700 disabled:opacity-40"
                        >
                          Delete
                        </button>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </>
      )}
    </section>
  );
}
