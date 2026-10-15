"use client";

import { useEffect, useRef, useState } from "react";

import type { ScenarioDefinition } from "@/lib/scenarios/types";

import {
  deleteSavedScenario,
  exportSavedScenarios,
  getSavedScenarios,
  importSavedScenarios,
  renameSavedScenario,
  saveScenario,
  type SavedScenario,
} from "@/lib/scenarios/storage";

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

export default function SavedScenarios({
  selectedScenario,
  onLoadScenario,
}: Props) {
  const [savedScenarios, setSavedScenarios] = useState<
    SavedScenario[]
  >([]);

  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);

  function refresh() {
    setSavedScenarios(getSavedScenarios());
  }

  useEffect(() => {
    refresh();
  }, []);

  function handleSave() {
    if (!selectedScenario) return;

    try {
      saveScenario(selectedScenario);
      refresh();

      setMessage(`Saved "${selectedScenario.name}".`);
      setError(null);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Unable to save scenario."
      );
    }
  }

  function handleLoad(scenario: SavedScenario) {
    onLoadScenario(scenario);

    setMessage(`Loaded "${scenario.name}".`);
    setError(null);
  }

  function handleRename(scenario: SavedScenario) {
    const name = window.prompt(
      "Rename saved scenario:",
      scenario.name
    );

    if (name === null) return;

    try {
      renameSavedScenario(scenario.id, name);
      refresh();

      setMessage("Scenario renamed.");
      setError(null);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Unable to rename scenario."
      );
    }
  }

  function handleDelete(scenario: SavedScenario) {
    const confirmed = window.confirm(
      `Delete saved scenario "${scenario.name}"?`
    );

    if (!confirmed) return;

    try {
      deleteSavedScenario(scenario.id);
      refresh();

      setMessage("Saved scenario deleted.");
      setError(null);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Unable to delete scenario."
      );
    }
  }

  function handleExport() {
    try {
      const json = exportSavedScenarios();

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

      setMessage("Saved scenarios exported.");
      setError(null);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Unable to export scenarios."
      );
    }
  }

  async function handleImport(
    event: React.ChangeEvent<HTMLInputElement>
  ) {
    const file = event.target.files?.[0];

    if (!file) return;

    try {
      const json = await file.text();

      const result = importSavedScenarios(json);

      refresh();

      setMessage(
        `Imported ${result.imported} scenario(s). ` +
          `${result.skipped} skipped.`
      );

      setError(null);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Unable to import scenarios."
      );
    } finally {
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    }
  }

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
            Save and reuse scenario definitions in this browser.
          </p>
        </div>

        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={handleSave}
            disabled={!selectedScenario}
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
            disabled={savedScenarios.length === 0}
            className="rounded-lg border border-gray-300 px-4 py-2 text-sm"
          >
            Export JSON
          </button>

          <button
            type="button"
            onClick={() => fileInputRef.current?.click()}
            className="rounded-lg border border-gray-300 px-4 py-2 text-sm"
          >
            Import JSON
          </button>

          <input
            ref={fileInputRef}
            type="file"
            accept=".json,application/json"
            onChange={handleImport}
            className="hidden"
          />
        </div>
      </div>

      {message && (
        <p className="mb-4 text-sm text-green-700">{message}</p>
      )}

      {error && (
        <p className="mb-4 text-sm text-red-700">{error}</p>
      )}

      {savedScenarios.length === 0 ? (
        <p className="rounded-lg bg-gray-50 px-4 py-6 text-sm text-gray-500">
          No saved scenarios yet. Run a scenario and select
          &quot;Save Selected&quot; to store it.
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
              </div>

              <div className="flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() => handleLoad(scenario)}
                  className="rounded-lg border border-gray-300 px-3 py-2 text-sm"
                >
                  Load
                </button>

                <button
                  type="button"
                  onClick={() => handleRename(scenario)}
                  className="rounded-lg border border-gray-300 px-3 py-2 text-sm"
                >
                  Rename
                </button>

                <button
                  type="button"
                  onClick={() => handleDelete(scenario)}
                  className="rounded-lg border border-red-200 px-3 py-2 text-sm text-red-700"
                >
                  Delete
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}