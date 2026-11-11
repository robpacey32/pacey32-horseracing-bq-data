
"use client";

import { useEffect, useRef, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";
import Link from "next/link";

import { auth } from "@/lib/firebase";
import type { ScenarioDefinition } from "@/lib/scenarios/types";
import {
  getSavedScenarios,
  type SavedScenario,
} from "@/lib/scenarios/storage";

import {
  getCloudScenarios,
  saveCloudScenario,
  renameCloudScenario,
  deleteCloudScenario,
  migrateLocalScenarios,
  updateScenarioTelegramSettings,
} from "@/lib/scenarios/cloudStorage";

interface Props {
  selectedScenario: ScenarioDefinition | null;
  onLoadScenario: (scenario: ScenarioDefinition) => void;
}

function formatDate(value: string) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) return "Unknown";

  return date.toLocaleDateString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  });
}

function isScenario(value: unknown): value is SavedScenario {
  if (!value || typeof value !== "object") return false;

  const item = value as Record<string, unknown>;

  return (
    typeof item.id === "string" &&
    item.id.length > 0 &&
    typeof item.name === "string" &&
    item.name.trim().length > 0 &&
    typeof item.startDate === "string" &&
    typeof item.endDate === "string" &&
    typeof item.savedAt === "string" &&
    typeof item.updatedAt === "string" &&
    typeof item.filters === "object" &&
    item.filters !== null &&
    typeof item.stake === "number" &&
    Number.isFinite(item.stake) &&
    item.stake > 0 &&
    item.stake <= 100000 &&
    ["win", "each_way"].includes(String(item.betType)) &&
    ["per_horse", "per_race"].includes(
      String(item.stakeAllocation)
    ) &&
    ["total", "per_leg"].includes(String(item.stakeMode))
  );
}

export default function SavedScenarios({
  selectedScenario,
  onLoadScenario,
}: Props) {
  const [user, setUser] = useState<User | null>(null);
  const [checkingAuth, setCheckingAuth] = useState(true);

  const [savedScenarios, setSavedScenarios] = useState<
    SavedScenario[]
  >([]);

  const [localIds, setLocalIds] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);

  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);

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
    try {
      setLocalIds(
        getSavedScenarios().map((scenario) => scenario.id)
      );
    } catch {
      setLocalIds([]);
    }
  }, []);

  async function refresh(uid: string) {
    const scenarios = await getCloudScenarios(uid);

    if (auth.currentUser?.uid === uid) {
      setSavedScenarios(scenarios);
    }
  }

  function showError(err: unknown, fallback: string) {
    setMessage(null);
    setError(err instanceof Error ? err.message : fallback);
  }

  async function handleSave() {
    if (!user || !selectedScenario || busy) return;

    setBusy(true);
    setError(null);

    try {
      await saveCloudScenario(user.uid, selectedScenario);
      await refresh(user.uid);

      setMessage(
        `Saved "${selectedScenario.name}" to your account.`
      );
    } catch (err) {
      showError(err, "Unable to save scenario.");
    } finally {
      setBusy(false);
    }
  }

  function handleLoad(scenario: SavedScenario) {
    onLoadScenario(scenario);
    setMessage(`Loaded "${scenario.name}".`);
    setError(null);
  }

  async function handleRename(scenario: SavedScenario) {
    if (!user || busy) return;

    const name = window.prompt(
      "Rename saved scenario:",
      scenario.name
    );

    if (name === null) return;

    setBusy(true);
    setError(null);

    try {
      await renameCloudScenario(
        user.uid,
        scenario.id,
        name
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

    try {
      await deleteCloudScenario(user.uid, scenario.id);
      await refresh(user.uid);

      setMessage("Saved scenario deleted.");
    } catch (err) {
      showError(err, "Unable to delete scenario.");
    } finally {
      setBusy(false);
    }
  }

  async function handleTelegramSettings(
    scenario: SavedScenario,
    enabled: boolean,
    noSelections: boolean
  ) {
    if (!user || busy) return;

    setBusy(true);
    setError(null);

    try {
      await updateScenarioTelegramSettings(
        user.uid,
        scenario.id,
        enabled,
        noSelections
      );

      await refresh(user.uid);
      setMessage("Telegram preferences updated.");
    } catch (err) {
      showError(err, "Unable to update Telegram preferences.");
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

    if (unmigrated.length === 0) {
      setMessage("All local scenarios are already in your account.");
      return;
    }

    const confirmed = window.confirm(
      `Import ${unmigrated.length} browser-saved scenario(s) ` +
        "into your account? Existing cloud scenarios will be " +
        "preserved. Local copies will remain."
    );

    if (!confirmed) return;

    setBusy(true);
    setError(null);

    try {
      const result = await migrateLocalScenarios(
        user.uid,
        unmigrated
      );

      await refresh(user.uid);

      setMessage(
        `Migrated ${result.imported} scenario(s). ` +
          `${result.skipped} skipped. Local copies preserved.`
      );
    } catch (err) {
      showError(err, "Unable to migrate local scenarios.");
    } finally {
      setBusy(false);
    }
  }

  function handleExport() {
    try {
      const json = JSON.stringify(
        {
          format: "pacey32-horseracing-scenarios",
          version: 1,
          exportedAt: new Date().toISOString(),
          scenarios: savedScenarios,
        },
        null,
        2
      );

      const blob = new Blob([json], {
        type: "application/json",
      });

      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");

      link.href = url;
      link.download = "pacey32-saved-scenarios.json";

      document.body.appendChild(link);
      link.click();
      link.remove();

      URL.revokeObjectURL(url);

      setMessage("Cloud scenarios exported.");
      setError(null);
    } catch (err) {
      showError(err, "Unable to export scenarios.");
    }
  }

  async function handleImport(
    event: React.ChangeEvent<HTMLInputElement>
  ) {
    const file = event.target.files?.[0];

    if (!file || !user || busy) return;

    setBusy(true);
    setError(null);

    try {
      const json = await file.text();
      const parsed: unknown = JSON.parse(json);

      if (!parsed || typeof parsed !== "object") {
        throw new Error("Invalid scenario file.");
      }

      const data = parsed as Record<string, unknown>;

      if (
        data.format !== "pacey32-horseracing-scenarios" ||
        data.version !== 1 ||
        !Array.isArray(data.scenarios)
      ) {
        throw new Error("Unsupported scenario export format.");
      }

      const valid = data.scenarios.filter(isScenario);
      const invalidCount =
        data.scenarios.length - valid.length;

      const imported = valid.map((scenario) => ({
        ...scenario,
        id: crypto.randomUUID(),
        savedAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        telegramEnabled: false,
        telegramNoSelections: false,
      }));

      const result = await migrateLocalScenarios(
        user.uid,
        imported
      );

      await refresh(user.uid);

      setMessage(
        `Imported ${result.imported} scenario(s). ` +
          `${invalidCount + result.skipped} skipped.`
      );
    } catch (err) {
      showError(err, "Unable to import scenarios.");
    } finally {
      setBusy(false);

      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    }
  }

  const cloudIds = new Set(
    savedScenarios.map((scenario) => scenario.id)
  );

  const unmigratedCount = localIds.filter(
    (id) => !cloudIds.has(id)
  ).length;

  return (
    <section
      className="mb-7 rounded-xl border bg-white p-6 shadow-sm"
      style={{
        borderColor: "var(--racing-border, #ded8c9)",
      }}
    >
      <div className="mb-5 flex flex-wrap items-center justify-between gap-4">
        <div>
          <h2
            className="font-serif text-2xl"
            style={{
              color: "var(--racing-green-dark, #123d2d)",
            }}
          >
            Saved Scenarios
          </h2>

          <p className="mt-1 text-sm text-gray-500">
            Save and reuse scenarios across devices using your account.
          </p>
        </div>

        {user && (
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() => void handleSave()}
              disabled={!selectedScenario || busy || loading}
              className="rounded-lg px-4 py-2 text-sm font-semibold text-white disabled:opacity-40"
              style={{
                background: "var(--racing-green, #164734)",
              }}
            >
              Save Selected
            </button>

            <button
              type="button"
              onClick={handleExport}
              disabled={
                savedScenarios.length === 0 || busy || loading
              }
              className="rounded-lg border border-gray-300 px-4 py-2 text-sm disabled:opacity-40"
            >
              Export JSON
            </button>

            <button
              type="button"
              onClick={() => fileInputRef.current?.click()}
              disabled={busy || loading}
              className="rounded-lg border border-gray-300 px-4 py-2 text-sm disabled:opacity-40"
            >
              Import JSON
            </button>

            <input
              ref={fileInputRef}
              type="file"
              accept=".json,application/json"
              onChange={(event) => void handleImport(event)}
              className="hidden"
            />
          </div>
        )}
      </div>

      {message && (
        <p className="mb-4 text-sm text-green-700">
          {message}
        </p>
      )}

      {error && (
        <p
          role="alert"
          className="mb-4 text-sm text-red-700"
        >
          {error}
        </p>
      )}

      {checkingAuth ? (
        <p className="text-sm text-gray-500">
          Checking account...
        </p>
      ) : !user ? (
        <div className="rounded-lg bg-gray-50 p-5 text-sm text-gray-600">
          You can run and compare scenarios without signing in.
          To save scenarios across devices,{" "}
          <Link
            href="/account"
            className="font-semibold text-green-800 underline"
          >
            sign in to your account
          </Link>
          .
        </div>
      ) : (
        <>
          {!loading && unmigratedCount > 0 && (
            <div className="mb-5 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-gray-200 bg-gray-50 p-4">
              <p className="text-sm text-gray-600">
                {unmigratedCount} browser-saved scenario(s)
                not yet in your account. You can copy them
                without deleting the originals.
              </p>

              <button
                type="button"
                onClick={() => void handleMigration()}
                disabled={busy || loading}
                className="rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm font-medium disabled:opacity-40"
              >
                Migrate Local Scenarios
              </button>
            </div>
          )}

          {loading ? (
            <p className="text-sm text-gray-500">
              Loading cloud scenarios...
            </p>
          ) : savedScenarios.length === 0 ? (
            <p className="rounded-lg bg-gray-50 px-4 py-6 text-sm text-gray-500">
              No cloud scenarios saved yet. Run a scenario and
              select &quot;Save Selected&quot; to store it in your account.
            </p>
          ) : (
            <div className="space-y-3">
              {savedScenarios.map((scenario) => (
                <div
                  key={scenario.id}
                  className="flex flex-wrap items-center justify-between gap-4 rounded-lg border border-gray-200 p-4"
                >
                  <div>
                    <div className="font-semibold">
                      {scenario.name}
                    </div>

                    <div className="mt-1 text-sm text-gray-500">
                      {scenario.startDate} to {scenario.endDate}
                      {" · "}
                      Saved {formatDate(scenario.savedAt)}
                    </div>

                    {/* Telegram preferences */}
                    <div className="mt-3 space-y-2">
                      <label className="flex items-center gap-2 text-sm text-gray-500">
                        <input
                          type="checkbox"
                          checked={
                            scenario.telegramEnabled ?? false
                          }
                          disabled
                          onChange={() =>
                            void handleTelegramSettings(
                              scenario,
                              !scenario.telegramEnabled,
                              scenario.telegramNoSelections ?? false
                            )
                          }
                        />
                        Daily Telegram alerts
                      </label>

                      <label className="flex items-center gap-2 text-sm text-gray-500">
                        <input
                          type="checkbox"
                          checked={
                            scenario.telegramNoSelections ?? false
                          }
                          disabled
                          onChange={() =>
                            void handleTelegramSettings(
                              scenario,
                              scenario.telegramEnabled ?? false,
                              !scenario.telegramNoSelections
                            )
                          }
                        />
                        Notify when no horses qualify
                      </label>

                      <p className="text-xs text-gray-500">
                        Telegram account linking will be available soon.
                      </p>
                    </div>
                  </div>

                  <div className="flex flex-wrap gap-2">
                    <button
                      type="button"
                      onClick={() => handleLoad(scenario)}
                      disabled={busy}
                      className="rounded-lg border border-gray-300 px-3 py-2 text-sm disabled:opacity-40"
                    >
                      Load
                    </button>

                    <button
                      type="button"
                      onClick={() =>
                        void handleRename(scenario)
                      }
                      disabled={busy}
                      className="rounded-lg border border-gray-300 px-3 py-2 text-sm disabled:opacity-40"
                    >
                      Rename
                    </button>

                    <button
                      type="button"
                      onClick={() =>
                        void handleDelete(scenario)
                      }
                      disabled={busy}
                      className="rounded-lg border border-red-200 px-3 py-2 text-sm text-red-700 disabled:opacity-40"
                    >
                      Delete
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </section>
  );
}
