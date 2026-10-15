"use client";

import { useEffect, useState } from "react";
import type { ScenarioDefinition, ScenarioSummary } from "@/lib/scenarios/types";

type ResultFilter = "all" | "winners" | "losers";

interface Selection {
  date: string;
  course: string | null;
  raceTime: string | null;
  horseName: string | null;
  odds: number;
  position: number;
  isWinner: boolean;
  staked: number;
  returns: number;
  profit: number;
}

interface SelectionsResponse {
  selections: Selection[];
  pagination: {
    page: number;
    pageSize: number;
    totalItems: number;
    totalPages: number;
  };
  totals: {
    selections: number;
    winners: number;
    staked: number;
    returns: number;
    profit: number;
  };
}

interface Props {
  scenario: ScenarioDefinition | null;
  summary?: ScenarioSummary | null;
}

const PAGE_SIZE = 50;

function formatNumber(value: number, decimals = 2) {
  return value.toLocaleString("en-GB", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

export default function ScenarioSelections({
  scenario,
  summary,
}: Props) {
  const [page, setPage] = useState(1);
  const [resultFilter, setResultFilter] = useState<ResultFilter>("all");
  const [data, setData] = useState<SelectionsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const scenarioKey = scenario ? JSON.stringify(scenario) : "";

  useEffect(() => {
    if (!scenario) return;

    const controller = new AbortController();

    async function loadSelections() {
      setLoading(true);
      setError(null);

      try {
        const response = await fetch("/api/scenarios/selections", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          signal: controller.signal,
          body: JSON.stringify({
            startDate: scenario!.startDate,
            endDate: scenario!.endDate,
            stake: scenario!.stake,
            betType: scenario!.betType,
            stakeAllocation: scenario!.stakeAllocation,
            stakeMode: scenario!.stakeMode,
            filters: scenario!.filters,
            page,
            pageSize: PAGE_SIZE,
            resultFilter,
          }),
        });

        const result = await response.json();

        if (!response.ok) {
          throw new Error(result.error || "Failed to load selections.");
        }

        if (!controller.signal.aborted) {
          setData(result as SelectionsResponse);
        }
      } catch (err) {
        if (!controller.signal.aborted) {
          setError(
            err instanceof Error ? err.message : "Unexpected error."
          );
        }
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      }
    }

    void loadSelections();

    return () => controller.abort();
    // scenarioKey tracks changes to the scenario definition.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scenarioKey, page, resultFilter]);

  if (!scenario) return null;

  const reconciles =
    data &&
    summary &&
    data.totals.selections === summary.bets &&
    Math.abs(data.totals.staked - summary.staked) < 0.02 &&
    Math.abs(data.totals.returns - summary.returns) < 0.02 &&
    Math.abs(data.totals.profit - summary.profit) < 0.02;

  function exportPage() {
    if (!data?.selections.length) return;

    const columns: Array<keyof Selection> = [
      "date",
      "course",
      "raceTime",
      "horseName",
      "odds",
      "position",
      "staked",
      "returns",
      "profit",
    ];

    const escape = (value: unknown) =>
      `"${String(value ?? "").replaceAll('"', '""')}"`;

    const csv = [
      columns.join(","),
      ...data.selections.map((row) =>
        columns.map((column) => escape(row[column])).join(",")
      ),
    ].join("\r\n");

    const blob = new Blob([csv], {
      type: "text/csv;charset=utf-8;",
    });

    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");

    link.href = url;
    link.download = `scenario-selections-page-${page}.csv`;
    link.click();

    URL.revokeObjectURL(url);
  }

  return (
    <section className="mb-7 rounded-xl border border-gray-200 bg-white p-6">
      <div className="mb-5 flex flex-wrap items-center justify-between gap-4">
        <div>
          <h2 className="font-serif text-2xl">Historical Selections</h2>
          <p className="mt-1 text-sm text-gray-500">
            {scenario.name}
          </p>
        </div>

        <div className="flex gap-2">
          <select
            value={resultFilter}
            onChange={(event) => {
              setPage(1);
              setResultFilter(event.target.value as ResultFilter);
            }}
            className="rounded-lg border border-gray-300 px-3 py-2 text-sm"
          >
            <option value="all">All selections</option>
            <option value="winners">Winners</option>
            <option value="losers">Losers</option>
          </select>

          <button
            type="button"
            onClick={exportPage}
            disabled={!data?.selections.length || loading}
            className="rounded-lg border border-gray-300 px-3 py-2 text-sm disabled:opacity-50"
          >
            Export page CSV
          </button>
        </div>
      </div>

      {data && (
        <div className="mb-4 flex flex-wrap gap-4 text-sm text-gray-600">
          <span>
            <strong>{formatNumber(data.pagination.totalItems, 0)}</strong>{" "}
            matching selections
          </span>

          <span>
            Total profit:{" "}
            <strong>{formatNumber(data.totals.profit)}</strong>
          </span>

          {summary && (
            <span
              className={
                reconciles ? "text-green-700" : "text-red-700"
              }
            >
              {reconciles
                ? "Totals match scenario summary"
                : "Totals differ from scenario summary"}
            </span>
          )}
        </div>
      )}

      {loading && (
        <p className="py-4 text-sm text-gray-500">
          Loading selections...
        </p>
      )}

      {error && (
        <p className="py-4 text-sm text-red-700">{error}</p>
      )}

      {!loading && !error && data && (
        <>
          <div className="overflow-x-auto">
            <table className="w-full min-w-[850px] text-sm">
              <thead className="bg-gray-50">
                <tr>
                  {[
                    "Date",
                    "Course",
                    "Time",
                    "Horse",
                    "Odds",
                    "Position",
                    "Stake",
                    "Returns",
                    "Profit",
                  ].map((heading) => (
                    <th
                      key={heading}
                      className="px-3 py-3 text-left font-semibold"
                    >
                      {heading}
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody className="divide-y divide-gray-100">
                {data.selections.map((selection, index) => (
                  <tr key={`${selection.date}-${selection.raceTime}-${selection.horseName}-${index}`}>
                    <td className="whitespace-nowrap px-3 py-3">
                      {selection.date}
                    </td>
                    <td className="px-3 py-3">{selection.course}</td>
                    <td className="px-3 py-3">{selection.raceTime}</td>
                    <td className="px-3 py-3 font-medium">
                      {selection.horseName}
                    </td>
                    <td className="px-3 py-3">
                      {formatNumber(selection.odds)}
                    </td>
                    <td className="px-3 py-3">
                      {selection.position === 999
                        ? "DNF"
                        : selection.position}
                    </td>
                    <td className="px-3 py-3">
                      {formatNumber(selection.staked)}
                    </td>
                    <td className="px-3 py-3">
                      {formatNumber(selection.returns)}
                    </td>
                    <td
                      className={`px-3 py-3 font-semibold ${
                        selection.profit < 0
                          ? "text-red-700"
                          : "text-green-700"
                      }`}
                    >
                      {formatNumber(selection.profit)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="mt-5 flex items-center justify-between">
            <button
              type="button"
              disabled={page <= 1}
              onClick={() => setPage((previous) => previous - 1)}
              className="rounded-lg border px-4 py-2 text-sm disabled:opacity-40"
            >
              Previous
            </button>

            <span className="text-sm text-gray-600">
              Page {page} of {data.pagination.totalPages || 1}
            </span>

            <button
              type="button"
              disabled={page >= data.pagination.totalPages}
              onClick={() => setPage((previous) => previous + 1)}
              className="rounded-lg border px-4 py-2 text-sm disabled:opacity-40"
            >
              Next
            </button>
          </div>
        </>
      )}
    </section>
  );
}