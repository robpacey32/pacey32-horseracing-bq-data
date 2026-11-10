
"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import VisualExplorer from "./VisualExplorer";
import MomentumExplorer from "./MomentumExplorer";
import Link from "next/link";

type EntityType = "HORSE" | "TRAINER" | "JOCKEY";
type Currency = "GBP" | "EUR";
type ActiveTab = EntityType | "EXPLORER" | "MOMENTUM";

type SortKey =
  | "name"
  | "runs"
  | "wins"
  | "places"
  | "winRate"
  | "placeRate"
  | "averageFinish"
  | "averageFinishPercentile"
  | "prizeMoney"
  | "prizePerRun";

type LeaderboardRow = {
  name: string;
  runs: number;
  wins: number;
  places: number;
  winRate: number;
  placeRate: number;
  averageFinish: number | null;
  averageFinishPercentile: number | null;
  prizeMoney: number;
};

type DisplayRow = LeaderboardRow & {
  prizePerRun: number;
};

type ResponseData = {
  rows: LeaderboardRow[];
  exchangeRate: {
    date: string;
    gbpToEur: number;
  };
};

const today = new Date().toISOString().slice(0, 10);

export default function AnalyticsPage() {
  const [activeTab, setActiveTab] = useState<ActiveTab>("TRAINER");
  const [entityType, setEntityType] = useState<EntityType>("TRAINER");
  const [currency, setCurrency] = useState<Currency>("GBP");
  const [startDate, setStartDate] = useState("2026-01-01");
  const [endDate, setEndDate] = useState(today);
  const [minRuns, setMinRuns] = useState(10);
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("prizeMoney");
  const [descending, setDescending] = useState(true);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [data, setData] = useState<ResponseData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const load = useCallback(
    async (signal: AbortSignal) => {
      if (startDate > endDate) {
        setData(null);
        setError("Start date must be before end date.");
        setLoading(false);
        return;
      }

      setLoading(true);
      setError("");

      try {
        const query = new URLSearchParams({
          entityType,
          currency,
          startDate,
          endDate,
        });

        const response = await fetch(
          `/api/analytics/leaderboard?${query}`,
          { signal }
        );

        const result = await response.json();

        if (!response.ok) {
          throw new Error(result.error ?? "Request failed");
        }

        if (!signal.aborted) {
          setData(result);
        }
      } catch (err) {
        if (!signal.aborted) {
          setError(
            err instanceof Error ? err.message : String(err)
          );
          setData(null);
        }
      } finally {
        if (!signal.aborted) {
          setLoading(false);
        }
      }
    },
    [entityType, currency, startDate, endDate]
  );

  useEffect(() => {
    // Momentum has its own API and fetch lifecycle.
    if (activeTab === "MOMENTUM") return;

    const controller = new AbortController();
    void load(controller.signal);

    return () => controller.abort();
  }, [load, activeTab]);

  const rows = useMemo<DisplayRow[]>(() => {
    const filtered = (data?.rows ?? [])
      .map((row) => ({
        ...row,
        prizePerRun:
          row.runs > 0 ? row.prizeMoney / row.runs : 0,
      }))
      .filter(
        (row) =>
          row.runs >= minRuns &&
          row.name.toLowerCase().includes(search.toLowerCase())
      );

    return filtered.sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];

      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;

      const comparison =
        typeof av === "string"
          ? av.localeCompare(String(bv))
          : Number(av) - Number(bv);

      return descending ? -comparison : comparison;
    });
  }, [data, minRuns, search, sortKey, descending]);

  const totalPages = Math.max(
    1,
    Math.ceil(rows.length / pageSize)
  );

  const currentPage = Math.min(page, totalPages);

  const paginatedRows = rows.slice(
    (currentPage - 1) * pageSize,
    currentPage * pageSize
  );

  function resetPage() {
    setPage(1);
  }

  function selectEntity(type: EntityType) {
    setEntityType(type);
    setActiveTab(type);
    resetPage();
  }

  function changeSort(key: SortKey) {
    if (sortKey === key) {
      setDescending(!descending);
    } else {
      setSortKey(key);
      setDescending(
        key !== "name" && key !== "averageFinish"
      );
    }

    resetPage();
  }

  const number = (n: number) =>
    n.toLocaleString("en-GB");

  const money = (n: number) =>
    new Intl.NumberFormat("en-GB", {
      style: "currency",
      currency,
      maximumFractionDigits: 0,
    }).format(n);

  const columns: { key: SortKey; label: string }[] = [
    {
      key: "name",
      label:
        entityType === "HORSE"
          ? "Horse"
          : entityType === "TRAINER"
          ? "Trainer"
          : "Jockey",
    },
    { key: "runs", label: "Runs" },
    { key: "wins", label: "Wins" },
    { key: "places", label: "Paid places" },
    { key: "winRate", label: "Win %" },
    { key: "placeRate", label: "Place %" },
    { key: "averageFinish", label: "Avg finish" },
    {
      key: "averageFinishPercentile",
      label: "Finish score",
    },
    { key: "prizeMoney", label: "Prize money" },
    { key: "prizePerRun", label: "Prize / run" },
  ];

  const totalRuns = rows.reduce(
    (sum, row) => sum + row.runs,
    0
  );

  const totalWins = rows.reduce(
    (sum, row) => sum + row.wins,
    0
  );

  const totalPrizeMoney = rows.reduce(
    (sum, row) => sum + row.prizeMoney,
    0
  );

  const firstResult =
    rows.length === 0
      ? 0
      : (currentPage - 1) * pageSize + 1;

  const lastResult = Math.min(
    currentPage * pageSize,
    rows.length
  );

  return (
    <main className="mx-auto max-w-7xl px-4 py-8">
      <h1 className="text-3xl font-bold">
        Racing Analytics
      </h1>

      <p className="mt-2 text-sm text-gray-500">
        Historical racing performance and leaderboards
      </p>

      {/* Main tabs */}
      <div className="mt-6 flex flex-wrap gap-2">
        {(["HORSE", "TRAINER", "JOCKEY"] as EntityType[]).map(
          (type) => (
            <button
              key={type}
              onClick={() => selectEntity(type)}
              className={`rounded-lg px-5 py-2 text-sm font-semibold ${
                activeTab === type
                  ? "bg-blue-700 text-white"
                  : "bg-gray-100 text-gray-800"
              }`}
            >
              {type === "HORSE"
                ? "Horses"
                : type === "TRAINER"
                ? "Trainers"
                : "Jockeys"}
            </button>
          )
        )}

        <button
          onClick={() => setActiveTab("EXPLORER")}
          className={`rounded-lg px-5 py-2 text-sm font-semibold ${
            activeTab === "EXPLORER"
              ? "bg-blue-700 text-white"
              : "bg-gray-100 text-gray-800"
          }`}
        >
          Visual Explorer
        </button>

        <button
          onClick={() => setActiveTab("MOMENTUM")}
          className={`rounded-lg px-5 py-2 text-sm font-semibold ${
            activeTab === "MOMENTUM"
              ? "bg-blue-700 text-white"
              : "bg-gray-100 text-gray-800"
          }`}
        >
          Who&apos;s Hot? / Cold?
        </button>
      </div>

      {/* Shared filters */}
      <section className="mt-6 flex flex-wrap items-end gap-4 rounded-xl border p-4">
        {activeTab === "EXPLORER" && (
          <label className="flex flex-col gap-1 text-sm">
            Analyse
            <select
              value={entityType}
              onChange={(e) => {
                setEntityType(e.target.value as EntityType);
                resetPage();
              }}
              className="rounded border px-2 py-2"
            >
              <option value="HORSE">Horses</option>
              <option value="TRAINER">Trainers</option>
              <option value="JOCKEY">Jockeys</option>
            </select>
          </label>
        )}

        {activeTab !== "MOMENTUM" && (
          <>
            <label className="flex flex-col gap-1 text-sm">
              From
              <input
                type="date"
                value={startDate}
                onChange={(e) => {
                  setStartDate(e.target.value);
                  resetPage();
                }}
                className="rounded border px-2 py-2"
              />
            </label>

            <label className="flex flex-col gap-1 text-sm">
              To
              <input
                type="date"
                value={endDate}
                onChange={(e) => {
                  setEndDate(e.target.value);
                  resetPage();
                }}
                className="rounded border px-2 py-2"
              />
            </label>

            <label className="flex flex-col gap-1 text-sm">
              Minimum runs
              <input
                type="number"
                min={0}
                value={minRuns}
                onChange={(e) => {
                  setMinRuns(
                    Math.max(0, Number(e.target.value))
                  );
                  resetPage();
                }}
                className="w-28 rounded border px-2 py-2"
              />
            </label>
          </>
        )}

        <label className="flex flex-col gap-1 text-sm">
          Currency
          <select
            value={currency}
            onChange={(e) => {
              setCurrency(e.target.value as Currency);
              resetPage();
            }}
            className="rounded border px-2 py-2"
          >
            <option value="GBP">GBP (£)</option>
            <option value="EUR">EUR (€)</option>
          </select>
        </label>

        {activeTab !== "MOMENTUM" && (
          <label className="flex flex-col gap-1 text-sm">
            Search
            <input
              value={search}
              onChange={(e) => {
                setSearch(e.target.value);
                resetPage();
              }}
              placeholder="Horse, trainer or jockey"
              className="rounded border px-2 py-2"
            />
          </label>
        )}

        {activeTab === "MOMENTUM" && (
          <label className="flex flex-col gap-1 text-sm">
            Comparison end date
            <input
              type="date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="rounded border px-2 py-2"
            />
          </label>
        )}
      </section>

      {/* Momentum uses its own API */}
      {activeTab === "MOMENTUM" && (
        <MomentumExplorer
          currency={currency}
          endDate={endDate}
        />
      )}

      {/* Leaderboard and Visual Explorer */}
      {activeTab !== "MOMENTUM" && (
        <>
          {error && (
            <p className="mt-4 text-red-600">{error}</p>
          )}

          {loading && (
            <p className="mt-4 text-sm">
              Loading analytics...
            </p>
          )}

          {data && !loading && (
            <>
              {activeTab === "EXPLORER" ? (
                <>
                  {/* Visual Explorer */}
                  <VisualExplorer
                    rows={rows}
                    currency={currency}
                  />

                  <div className="mt-4 space-y-2 text-xs text-gray-500">
                    <p>
                      Each dot represents one{" "}
                      {entityType === "HORSE"
                        ? "horse"
                        : entityType === "TRAINER"
                        ? "trainer"
                        : "jockey"}
                      . The dashed lines show median
                      values for the selected metrics.
                    </p>

                    <p>
                      Showing {rows.length} entities
                      from the top 500 ranked by total
                      prize money. Minimum runs and
                      search filters are applied before
                      plotting.
                    </p>

                    {data.exchangeRate && (
                      <p>
                        ECB exchange rate dated{" "}
                        {data.exchangeRate.date}.
                      </p>
                    )}
                  </div>
                </>
              ) : (
                <>
                  {/* Summary cards */}
                  <div className="mt-5 grid grid-cols-2 gap-3 md:grid-cols-4">
                    {[
                      ["Ranked", number(rows.length)],
                      ["Total runs", number(totalRuns)],
                      ["Total wins", number(totalWins)],
                      ["Prize money", money(totalPrizeMoney)],
                    ].map(([label, value]) => (
                      <div
                        key={label}
                        className="rounded-xl border p-4"
                      >
                        <div className="text-xs text-gray-500">
                          {label}
                        </div>
                        <div className="mt-1 text-xl font-bold">
                          {value}
                        </div>
                      </div>
                    ))}
                  </div>

                  {/* Leaderboard */}
                  <div className="mt-5 overflow-x-auto rounded-xl border">
                    <table className="w-full whitespace-nowrap text-sm">
                      <thead className="bg-gray-100">
                        <tr>
                          <th className="px-3 py-3 text-left">
                            #
                          </th>

                          {columns.map((column) => (
                            <th
                              key={column.key}
                              className="px-3 py-3 text-left"
                            >
                              <button
                                onClick={() =>
                                  changeSort(column.key)
                                }
                                title={
                                  column.key ===
                                  "averageFinishPercentile"
                                    ? "Average finishing position adjusted for field size. 100 = winner; 0 = last. Non-finishers excluded."
                                    : column.key ===
                                      "prizePerRun"
                                    ? "Total prize money divided by number of runs."
                                    : undefined
                                }
                              >
                                {column.label}
                                {sortKey === column.key
                                  ? descending
                                    ? " ↓"
                                    : " ↑"
                                  : ""}
                              </button>
                            </th>
                          ))}
                        </tr>
                      </thead>

                      <tbody>
                        {paginatedRows.map(
                          (row, index) => (
                            <tr
                              key={row.name}
                              className="border-t hover:bg-gray-50"
                            >
                              <td className="px-3 py-3">
                                {(currentPage - 1) *
                                  pageSize +
                                  index +
                                  1}
                              </td>

                              <td className="px-3 py-3 font-semibold">
                                <Link
                                  href={`/analytics/drilldown?${new URLSearchParams({
                                    type: entityType,
                                    name: row.name,
                                    startDate,
                                    endDate,
                                    currency,
                                  }).toString()}`}
                                  className="text-blue-700 hover:underline"
                                >
                                  {row.name}
                                </Link>
                              </td>

                              <td className="px-3 py-3">
                                {number(row.runs)}
                              </td>

                              <td className="px-3 py-3">
                                {number(row.wins)}
                              </td>

                              <td className="px-3 py-3">
                                {number(row.places)}
                              </td>

                              <td className="px-3 py-3">
                                {row.winRate.toFixed(1)}%
                              </td>

                              <td className="px-3 py-3">
                                {row.placeRate.toFixed(1)}%
                              </td>

                              <td className="px-3 py-3">
                                {row.averageFinish?.toFixed(
                                  2
                                ) ?? "—"}
                              </td>

                              <td className="px-3 py-3">
                                {row.averageFinishPercentile?.toFixed(
                                  1
                                ) ?? "—"}
                              </td>

                              <td className="px-3 py-3">
                                {money(row.prizeMoney)}
                              </td>

                              <td className="px-3 py-3">
                                {money(row.prizePerRun)}
                              </td>
                            </tr>
                          )
                        )}
                      </tbody>
                    </table>
                  </div>

                  {/* Pagination */}
                  <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
                    <label className="flex items-center gap-2 text-sm">
                      Show
                      <select
                        value={pageSize}
                        onChange={(e) => {
                          setPageSize(
                            Number(e.target.value)
                          );
                          resetPage();
                        }}
                        className="rounded border px-2 py-2"
                      >
                        <option value={10}>10</option>
                        <option value={20}>20</option>
                        <option value={50}>50</option>
                      </select>
                      results per page
                    </label>

                    <p className="text-sm text-gray-500">
                      Showing {firstResult}–
                      {lastResult} of {rows.length}
                    </p>

                    <div className="flex items-center gap-2">
                      <button
                        disabled={currentPage === 1}
                        onClick={() =>
                          setPage(currentPage - 1)
                        }
                        className="rounded border px-3 py-2 text-sm disabled:opacity-40"
                      >
                        Previous
                      </button>

                      <span className="px-2 text-sm">
                        Page {currentPage} of{" "}
                        {totalPages}
                      </span>

                      <button
                        disabled={
                          currentPage >= totalPages
                        }
                        onClick={() =>
                          setPage(currentPage + 1)
                        }
                        className="rounded border px-3 py-2 text-sm disabled:opacity-40"
                      >
                        Next
                      </button>
                    </div>
                  </div>

                  {/* Metric explanations */}
                  <div className="mt-4 space-y-2 text-xs text-gray-500">
                    <p>
                      <strong>Prize / run:</strong>{" "}
                      Total prize money divided by
                      the number of runs or rides.
                      Useful for comparing entities
                      with different levels of activity.
                    </p>

                    <p>
                      <strong>Finish score:</strong>{" "}
                      Average finishing performance
                      adjusted for field size.
                      A winner scores 100 and the
                      last finisher scores 0.
                      For example, finishing 3rd
                      in a 10-runner race scores 77.8.
                      Non-finishers are excluded.
                    </p>

                    <p>
                      <strong>Paid places:</strong>{" "}
                      Includes winners and other
                      paid finishing positions
                      based on the existing race
                      analytics rules.
                    </p>

                    <p>
                      Showing {rows.length} entities
                      from the top 500 ranked by
                      total prize money.
                      {data.exchangeRate &&
                        ` ECB exchange rate dated ${data.exchangeRate.date}.`}
                    </p>
                  </div>
                </>
              )}
            </>
          )}
        </>
      )}
    </main>
  );
}
