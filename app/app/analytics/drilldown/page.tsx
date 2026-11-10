
"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type EntityType = "HORSE" | "TRAINER" | "JOCKEY";
type Currency = "GBP" | "EUR";

type SortKey =
  | "date"
  | "location"
  | "horse"
  | "positionNumber"
  | "runners"
  | "prizeMoney"
  | "raceType"
  | "going";

type Race = {
  date: string;
  location: string | null;
  time: string | null;
  raceName: string | null;
  horse: string | null;
  trainer: string | null;
  jockey: string | null;
  position: string | null;
  positionNumber: number | null;
  runners: number | null;
  won: boolean;
  placed: boolean;
  finishScore: number | null;
  prizeMoney: number;
  raceType: string | null;
  going: string | null;
  surface: string | null;
  rideDescription: string | null;
  sp: string | null;
  favouriteStatus: string | null;
  courseDistance: string | null;
  beatenFavourite: string | null;
  raceDistance: string | null;
  raceClass: string | null;
  weight: string | null;
  age: string | null;
  headgear: string | null;
  lastRun: string | null;
  winningTime: string | null;
  sourceUrl: string | null;
};

type Trend = {
  month: string;
  monthLabel: string;
  runs: number;
  wins: number;
  places: number;
  prizeMoney: number;
  prizePerRun: number | null;
  winRate: number | null;
  placeRate: number | null;
};

type Summary = {
  runs: number;
  wins: number;
  seconds: number;
  places: number;
  winRate: number;
  placeRate: number;
  finishScore: number | null;
  prizeMoney: number;
  prizePerRun: number;
};

type DrilldownData = {
  type: EntityType;
  name: string;
  currency: Currency;
  summary: Summary;
  trends: Trend[];
  races: Race[];
};

type KPI = {
  label: string;
  value: string;
  icon: string;
  sub?: string;
  featured?: boolean;
  percentage?: number;
};

const GREEN = "#003B2B";
const GOLD = "#B38B32";
const today = new Date().toISOString().slice(0, 10);

function formatDate(value: string) {
  const [year, month, day] = value.split("-");
  return `${day}/${month}/${year}`;
}

function formatLastRun(value: string | null) {
  if (!value?.trim()) return null;

  const trimmed = value.trim();

  if (/^\d+$/.test(trimmed)) {
    return `${trimmed} days`;
  }

  return trimmed;
}

function RateRing({ value }: { value: number }) {
  const percentage = Math.max(0, Math.min(100, value));

  return (
    <div
      className="relative h-9 w-9 shrink-0 rounded-full"
      style={{
        background: `conic-gradient(${GREEN} ${percentage}%, #D9D9D9 ${percentage}% 100%)`,
      }}
      title={`${percentage.toFixed(1)}%`}
      aria-label={`${percentage.toFixed(1)} percent`}
      role="img"
    >
      <div className="absolute inset-[6px] rounded-full bg-[#FAF8F2]" />
    </div>
  );
}

function PerformanceCard({ item }: { item: KPI }) {
  return (
    <div
      className={`flex min-h-[128px] min-w-0 flex-col rounded-xl p-3 text-[#003B2B] shadow-sm transition-transform hover:-translate-y-1 ${
        item.featured
          ? "bg-gradient-to-br from-amber-50 to-[#F5E3AB]"
          : "bg-[#FAF8F2]"
      }`}
    >
      <div className="flex items-start justify-between gap-1">
        <span className="text-[11px] font-semibold leading-tight text-gray-700">
          {item.label}
        </span>
        <span
          className="shrink-0 text-lg font-bold leading-none text-[#B38B32]"
          aria-hidden="true"
        >
          {item.icon}
        </span>
      </div>

      <div className="mt-auto">
        <div className="flex items-end justify-between gap-1">
          <span className="min-w-0 break-words text-xl font-extrabold tracking-tight sm:text-2xl xl:text-lg 2xl:text-xl">
            {item.value}
          </span>

          {item.percentage !== undefined && (
            <RateRing value={item.percentage} />
          )}
        </div>

        {item.sub && (
          <p className="mt-1 text-[11px] text-gray-600">
            {item.sub}
          </p>
        )}
      </div>
    </div>
  );
}

function DrilldownContent() {
  const searchParams = useSearchParams();

  const type = searchParams.get("type") as EntityType | null;
  const name = searchParams.get("name") ?? "";

  const [startDate, setStartDate] = useState(
    searchParams.get("startDate") ?? "2025-01-01"
  );

  const [endDate, setEndDate] = useState(
    searchParams.get("endDate") ?? today
  );

  const [currency, setCurrency] = useState<Currency>(
    searchParams.get("currency") === "EUR" ? "EUR" : "GBP"
  );

  const [data, setData] = useState<DrilldownData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [sortKey, setSortKey] = useState<SortKey>("date");
  const [descending, setDescending] = useState(true);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);

  const [selectedRace, setSelectedRace] = useState<Race | null>(
    null
  );

  useEffect(() => {
    if (!type || !name || startDate > endDate) {
      setData(null);
      setError("Invalid selection or date range.");
      return;
    }

    const controller = new AbortController();

    async function load() {
      setLoading(true);
      setError("");
      setSelectedRace(null);

      try {
        const params = new URLSearchParams({
          type: type as string,
          name,
          startDate,
          endDate,
          currency,
        });

        const response = await fetch(
          `/api/analytics/drilldown?${params}`,
          { signal: controller.signal }
        );

        const result = await response.json();

        if (!response.ok) {
          throw new Error(result.error ?? "Request failed");
        }

        if (!controller.signal.aborted) {
          setData(result);
          setPage(1);
        }
      } catch (err) {
        if (!controller.signal.aborted) {
          setError(
            err instanceof Error ? err.message : String(err)
          );
          setData(null);
        }
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      }
    }

    void load();

    return () => controller.abort();
  }, [type, name, startDate, endDate, currency]);

  useEffect(() => {
    if (!selectedRace) return;

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setSelectedRace(null);
      }
    }

    window.addEventListener("keydown", onKeyDown);

    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [selectedRace]);

  const sortedRaces = useMemo(() => {
    const races = [...(data?.races ?? [])];

    races.sort((a, b) => {
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

    return races;
  }, [data, sortKey, descending]);

  const totalPages = Math.max(
    1,
    Math.ceil(sortedRaces.length / pageSize)
  );

  const currentPage = Math.min(page, totalPages);

  const visibleRaces = sortedRaces.slice(
    (currentPage - 1) * pageSize,
    currentPage * pageSize
  );

  const money = (value: number) =>
    new Intl.NumberFormat("en-GB", {
      style: "currency",
      currency,
      maximumFractionDigits: 0,
    }).format(value);

  const compactMoney = (value: number) =>
    new Intl.NumberFormat("en-GB", {
      notation: "compact",
      maximumFractionDigits: 1,
    }).format(value);

  function changeSort(key: SortKey) {
    if (sortKey === key) {
      setDescending(!descending);
    } else {
      setSortKey(key);
      setDescending(
        !["location", "horse", "raceType", "going"].includes(
          key
        )
      );
    }

    setPage(1);
  }

  const raceColumns: { key: SortKey; label: string }[] = [
    { key: "date", label: "Date" },
    { key: "location", label: "Location" },
    { key: "horse", label: "Horse" },
    { key: "positionNumber", label: "Finish" },
    { key: "runners", label: "Runners" },
    { key: "prizeMoney", label: "Prize money" },
    { key: "raceType", label: "Flat / Jump" },
    { key: "going", label: "Going" },
  ];

  const kpis: KPI[] = data
    ? [
        {
          label: "Runs",
          value: data.summary.runs.toLocaleString(),
          icon: "⚑",
        },
        {
          label: "Wins",
          value: data.summary.wins.toLocaleString(),
          icon: "★",
          featured: true,
        },
        {
          label: "Paid places",
          value: data.summary.places.toLocaleString(),
          icon: "◈",
        },
        {
          label: "Win rate",
          value: `${data.summary.winRate.toFixed(1)}%`,
          icon: "↗",
          percentage: data.summary.winRate,
        },
        {
          label: "Place rate",
          value: `${data.summary.placeRate.toFixed(1)}%`,
          icon: "◎",
          percentage: data.summary.placeRate,
        },
        {
          label: "Finish score",
          value: data.summary.finishScore?.toFixed(1) ?? "—",
          icon: "☆",
        },
        {
          label: "Prize money",
          value: money(data.summary.prizeMoney),
          icon: currency === "GBP" ? "£" : "€",
        },
        {
          label: "Prize / run",
          value: money(data.summary.prizePerRun),
          icon: "◇",
        },
      ]
    : [];

  const detailFields: [string, string | null | undefined][] =
    selectedRace
      ? [
          ["Horse", selectedRace.horse],
          ["Finish", selectedRace.position],
          ["Runners", selectedRace.runners?.toString()],

          ["Starting price", selectedRace.sp],
          ["Favourite", selectedRace.favouriteStatus],
          ["Course/Distance", selectedRace.courseDistance],
          ["Beaten favourite", selectedRace.beatenFavourite],

          ["Race type", selectedRace.raceType],
          ["Distance", selectedRace.raceDistance],
          ["Class", selectedRace.raceClass],
          ["Going", selectedRace.going],
          ["Surface", selectedRace.surface],

          ["Weight", selectedRace.weight],
          ["Age", selectedRace.age],
          ["Headgear", selectedRace.headgear],
          ["Last run", formatLastRun(selectedRace.lastRun)],

          ["Jockey", selectedRace.jockey],
          ["Trainer", selectedRace.trainer],
          ["Winning time", selectedRace.winningTime],
          ["Prize money", money(selectedRace.prizeMoney)],
        ]
      : [];

  return (
    <main className="mx-auto max-w-7xl px-4 py-8">
      <Link
        href="/analytics"
        className="text-sm font-medium text-[#003B2B] hover:text-[#B38B32] hover:underline"
      >
        ← Back to Racing Analytics
      </Link>

      <h1 className="mt-5 text-3xl font-bold">
        {name || "Performance Drilldown"}
      </h1>

      <p className="mt-2 text-sm text-gray-500">
        {type === "HORSE"
          ? "Horse"
          : type === "TRAINER"
          ? "Trainer"
          : "Jockey"}{" "}
        performance analysis
      </p>

      {/* Filters */}
      <section className="mt-6 flex flex-wrap items-end gap-4 rounded-xl border p-4">
        <label className="flex flex-col gap-1 text-sm">
          From
          <input
            type="date"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
            className="rounded border px-3 py-2"
          />
        </label>

        <label className="flex flex-col gap-1 text-sm">
          To
          <input
            type="date"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            className="rounded border px-3 py-2"
          />
        </label>

        <label className="flex flex-col gap-1 text-sm">
          Currency
          <select
            value={currency}
            onChange={(e) =>
              setCurrency(e.target.value as Currency)
            }
            className="rounded border px-3 py-2"
          >
            <option value="GBP">GBP (£)</option>
            <option value="EUR">EUR (€)</option>
          </select>
        </label>
      </section>

      {loading && (
        <p className="mt-5 text-sm">Loading performance...</p>
      )}

      {error && (
        <p className="mt-5 text-sm text-red-600">{error}</p>
      )}

      {data && !loading && (
        <>
          {/* Performance overview */}
          <section className="mt-6 overflow-hidden rounded-2xl bg-[#003B2B] p-4 text-white shadow-lg sm:p-6">
            <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
              <div className="flex items-center gap-3">
                <svg
                  width="28"
                  height="28"
                  viewBox="0 0 24 24"
                  fill="none"
                  aria-hidden="true"
                >
                  <rect
                    x="2"
                    y="12"
                    width="5"
                    height="10"
                    rx="1"
                    fill="#B38B32"
                  />
                  <rect
                    x="9"
                    y="5"
                    width="5"
                    height="17"
                    rx="1"
                    fill="#D6B65E"
                  />
                  <rect
                    x="16"
                    y="9"
                    width="5"
                    height="13"
                    rx="1"
                    fill="#B38B32"
                  />
                </svg>

                <h2 className="text-lg font-bold tracking-wide sm:text-xl">
                  PERFORMANCE OVERVIEW
                </h2>
              </div>

              <span className="text-xs text-emerald-100/80">
                {formatDate(startDate)} – {formatDate(endDate)}
              </span>
            </div>

            {/* Compact KPI cards */}
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-4 xl:grid-cols-8">
              {kpis.map((item) => (
                <PerformanceCard
                  key={item.label}
                  item={item}
                />
              ))}
            </div>

            {/* Recent form */}
            <div className="mt-6 border-t border-white/30 pt-5">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="flex flex-wrap items-baseline gap-3">
                  <h3 className="text-lg font-bold">
                    Recent form
                  </h3>

                  <span className="text-xs text-emerald-100/80">
                    Last 10 results, newest first
                  </span>
                </div>

                <div className="flex flex-wrap items-center gap-4 text-xs text-white/90">
                  <span className="flex items-center gap-1.5">
                    <span className="h-3 w-3 rounded bg-[#B38B32]" />
                    Win
                  </span>

                  <span className="flex items-center gap-1.5">
                    <span className="h-3 w-3 rounded border border-amber-400 bg-[#FEF3C7]" />
                    Paid place
                  </span>

                  <span className="flex items-center gap-1.5">
                    <span className="h-3 w-3 rounded bg-white" />
                    Other
                  </span>
                </div>
              </div>

              <div className="mt-4 flex flex-wrap gap-2">
                {data.races.slice(0, 10).map((race, index) => (
                  <button
                    type="button"
                    key={`${race.date}-${race.time}-${index}`}
                    onClick={() => setSelectedRace(race)}
                    title={`${formatDate(race.date)} — ${
                      race.location ?? ""
                    } — ${race.horse ?? ""}`}
                    className={`flex h-12 w-12 items-center justify-center rounded-lg border text-lg font-bold shadow-sm transition-all hover:-translate-y-1 hover:shadow-md sm:h-14 sm:w-14 ${
                      race.won
                        ? "border-[#D6B65E] bg-[#B38B32] text-white"
                        : race.placed
                        ? "border-amber-400 bg-[#FEF3C7] text-amber-900"
                        : "border-gray-200 bg-white text-[#003B2B]"
                    }`}
                  >
                    {race.positionNumber ?? "—"}
                  </button>
                ))}
              </div>
            </div>
          </section>

          {/* Monthly charts */}
          <div className="mt-7 grid gap-5 lg:grid-cols-2">
            <section className="rounded-xl border p-4">
              <h2 className="mb-4 text-lg font-semibold">
                Monthly win and place %
              </h2>

              <div className="h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart
                    data={data.trends}
                    margin={{
                      top: 5,
                      right: 10,
                      left: -10,
                      bottom: 10,
                    }}
                  >
                    <CartesianGrid strokeDasharray="3 3" />

                    <XAxis
                      dataKey="monthLabel"
                      interval="preserveStartEnd"
                      minTickGap={20}
                      tick={{ fontSize: 11 }}
                    />

                    <YAxis
                      domain={[0, 100]}
                      unit="%"
                      tick={{ fontSize: 11 }}
                    />

                    <Tooltip
                      formatter={(value, label) => [
                        value == null
                          ? "No races"
                          : `${Number(value).toFixed(1)}%`,
                        String(label),
                      ]}
                    />

                    <Legend />

                    <Line
                      type="monotone"
                      dataKey="winRate"
                      name="Win %"
                      stroke={GOLD}
                      strokeWidth={2}
                      dot={false}
                      connectNulls={false}
                    />

                    <Line
                      type="monotone"
                      dataKey="placeRate"
                      name="Place %"
                      stroke={GREEN}
                      strokeWidth={2}
                      dot={false}
                      connectNulls={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </section>

            <section className="rounded-xl border p-4">
              <h2 className="mb-4 text-lg font-semibold">
                Monthly prize money
              </h2>

              <div className="h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart
                    data={data.trends}
                    margin={{
                      top: 5,
                      right: 10,
                      left: 0,
                      bottom: 10,
                    }}
                  >
                    <CartesianGrid strokeDasharray="3 3" />

                    <XAxis
                      dataKey="monthLabel"
                      interval="preserveStartEnd"
                      minTickGap={20}
                      tick={{ fontSize: 11 }}
                    />

                    <YAxis
                      yAxisId="total"
                      tickFormatter={(value) =>
                        compactMoney(Number(value))
                      }
                      tick={{ fontSize: 11 }}
                      width={50}
                    />

                    <YAxis
                      yAxisId="perRun"
                      orientation="right"
                      tickFormatter={(value) =>
                        compactMoney(Number(value))
                      }
                      tick={{ fontSize: 11 }}
                      width={50}
                    />

                    <Tooltip
                      formatter={(value, label) => [
                        value == null
                          ? "No races"
                          : money(Number(value)),
                        String(label),
                      ]}
                    />

                    <Legend />

                    <Line
                      yAxisId="total"
                      type="monotone"
                      dataKey="prizeMoney"
                      name="Total prize money"
                      stroke={GREEN}
                      strokeWidth={2}
                      dot={false}
                    />

                    <Line
                      yAxisId="perRun"
                      type="monotone"
                      dataKey="prizePerRun"
                      name="Prize money / run"
                      stroke={GOLD}
                      strokeWidth={2}
                      dot={false}
                      connectNulls={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>

              <div className="mt-2 flex justify-between text-xs text-gray-500">
                <span>Left axis: total prize money</span>
                <span>Right axis: prize / run</span>
              </div>
            </section>
          </div>

          {/* Race history */}
          <section className="mt-7">
            <h2 className="text-lg font-semibold">
              Race history
            </h2>

            <p className="mt-1 text-sm text-gray-500">
              {sortedRaces.length.toLocaleString()} recorded results
            </p>

            <div className="mt-4 overflow-x-auto rounded-xl border">
              <table className="w-full whitespace-nowrap text-sm">
                <thead className="bg-gray-100">
                  <tr>
                    {raceColumns.map((column) => (
                      <th
                        key={column.key}
                        className="px-3 py-3 text-left"
                      >
                        <button
                          type="button"
                          onClick={() =>
                            changeSort(column.key)
                          }
                          className="font-semibold hover:text-[#B38B32]"
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

                    <th className="px-3 py-3 text-left">
                      Details
                    </th>
                  </tr>
                </thead>

                <tbody>
                  {visibleRaces.map((race, index) => (
                    <tr
                      key={`${race.date}-${race.time}-${index}`}
                      className="border-t hover:bg-gray-50"
                    >
                      <td className="px-3 py-3">
                        {formatDate(race.date)}
                      </td>

                      <td className="px-3 py-3">
                        {race.location ?? "—"}
                      </td>

                      <td className="px-3 py-3">
                        {race.horse ?? "—"}
                      </td>

                      <td className="px-3 py-3">
                        <span
                          className={
                            race.won
                              ? "font-bold text-[#B38B32]"
                              : race.placed
                              ? "font-semibold text-amber-700"
                              : ""
                          }
                        >
                          {race.position ?? "—"}
                        </span>
                      </td>

                      <td className="px-3 py-3">
                        {race.runners ?? "—"}
                      </td>

                      <td className="px-3 py-3">
                        {money(race.prizeMoney)}
                      </td>

                      <td className="px-3 py-3">
                        {race.raceType ?? "—"}
                      </td>

                      <td className="px-3 py-3">
                        {race.going ?? "—"}
                      </td>

                      <td className="px-3 py-3">
                        <button
                          type="button"
                          onClick={() => setSelectedRace(race)}
                          className="font-medium text-[#003B2B] hover:text-[#B38B32] hover:underline"
                        >
                          More detail
                        </button>
                      </td>
                    </tr>
                  ))}
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
                    setPageSize(Number(e.target.value));
                    setPage(1);
                  }}
                  className="rounded border px-2 py-2"
                >
                  <option value={10}>10</option>
                  <option value={20}>20</option>
                  <option value={50}>50</option>
                </select>
                results per page
              </label>

              <span className="text-sm text-gray-500">
                Page {currentPage} of {totalPages}
              </span>

              <div className="flex gap-2">
                <button
                  type="button"
                  disabled={currentPage === 1}
                  onClick={() =>
                    setPage(currentPage - 1)
                  }
                  className="rounded border px-3 py-2 text-sm hover:border-[#B38B32] disabled:opacity-40"
                >
                  Previous
                </button>

                <button
                  type="button"
                  disabled={currentPage >= totalPages}
                  onClick={() =>
                    setPage(currentPage + 1)
                  }
                  className="rounded border px-3 py-2 text-sm hover:border-[#B38B32] disabled:opacity-40"
                >
                  Next
                </button>
              </div>
            </div>
          </section>
        </>
      )}

      {/* Race detail popup */}
      {selectedRace && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) {
              setSelectedRace(null);
            }
          }}
        >
          <div
            role="dialog"
            aria-modal="true"
            aria-labelledby="race-detail-title"
            className="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-2xl bg-white p-6 shadow-2xl"
          >
            <div className="flex items-start justify-between gap-4">
              <div>
                <p className="text-xs font-semibold uppercase tracking-wide text-[#B38B32]">
                  Race details
                </p>

                <h2
                  id="race-detail-title"
                  className="mt-1 text-xl font-bold text-[#003B2B]"
                >
                  {selectedRace.raceName ?? "Race"}
                </h2>

                <p className="mt-2 text-sm text-gray-500">
                  {formatDate(selectedRace.date)} ·{" "}
                  {selectedRace.location ?? "Unknown location"}
                  {selectedRace.time
                    ? ` · ${selectedRace.time}`
                    : ""}
                </p>
              </div>

              <button
                type="button"
                onClick={() => setSelectedRace(null)}
                aria-label="Close race details"
                className="rounded-lg border px-3 py-1 text-xl text-gray-600 hover:border-[#B38B32]"
              >
                ×
              </button>
            </div>

            {/* Race information */}
            <div className="mt-6 grid grid-cols-2 gap-4 rounded-xl bg-gray-50 p-4 text-sm sm:grid-cols-3">
              {detailFields.map(([label, value]) => (
                <div key={label}>
                  <div className="text-xs text-gray-500">
                    {label}
                  </div>

                  <div className="mt-1 font-semibold">
                    {value || "—"}
                  </div>
                </div>
              ))}
            </div>

            {/* Running commentary */}
            <div className="mt-6">
              <h3 className="text-base font-semibold text-[#003B2B]">
                Running commentary
              </h3>

              <p className="mt-2 whitespace-pre-wrap rounded-xl border-l-4 border-[#B38B32] bg-amber-50 p-4 text-sm leading-relaxed text-gray-800">
                {selectedRace.rideDescription?.trim() ||
                  "No running commentary available for this result."}
              </p>
            </div>

            {/* Race Viewer link */}
            {selectedRace.location && selectedRace.time && (
              <div className="mt-5">
                <Link
                  href={`/race-viewer?${new URLSearchParams({
                    date: selectedRace.date,
                    location: selectedRace.location,
                    time: selectedRace.time,
                  }).toString()}`}
                  className="text-sm font-semibold text-[#003B2B] hover:text-[#B38B32] hover:underline"
                >
                  View race in Race Viewer ↗
                </Link>
              </div>
            )}


            <div className="mt-6 flex justify-end">
              <button
                type="button"
                onClick={() => setSelectedRace(null)}
                className="rounded-lg bg-[#003B2B] px-5 py-2 text-sm font-medium text-white hover:bg-[#B38B32]"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </main>
  );
}

export default function DrilldownPage() {
  return (
    <Suspense fallback={<p className="p-8">Loading drilldown...</p>}>
      <DrilldownContent />
    </Suspense>
  );
}
