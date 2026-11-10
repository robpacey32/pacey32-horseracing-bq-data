
"use client";

import { useEffect, useMemo, useState } from "react";

type EntityType = "HORSE" | "TRAINER" | "JOCKEY";
type Currency = "GBP" | "EUR";
type Metric = "winRate" | "placeRate" | "finishScore" | "prizePerRun";
type Direction = "HOT" | "COLD";

type Stats = {
  runs: number;
  wins: number;
  places: number;
  winRate: number;
  placeRate: number;
  finishScore: number | null;
  prizeMoney: number;
  prizePerRun: number;
};

type MomentumRow = {
  name: string;
  current: Stats;
  previous: Stats;
  change: Record<Metric, number | null>;
};

type ApiResponse = {
  periods: {
    current: { start: string; end: string };
    previous: { start: string; end: string };
  };
  rows: MomentumRow[];
};

type Props = {
  currency: Currency;
  endDate: string;
};

const metrics: Record<Metric, string> = {
  winRate: "Win %",
  placeRate: "Place %",
  finishScore: "Finish score",
  prizePerRun: "Prize / run",
};

export default function MomentumExplorer({
  currency,
  endDate,
}: Props) {
  const [entityType, setEntityType] = useState<EntityType>("TRAINER");
  const [months, setMonths] = useState<1 | 3 | 6>(1);
  const [metric, setMetric] = useState<Metric>("winRate");
  const [direction, setDirection] = useState<Direction>("HOT");
  const [minRuns, setMinRuns] = useState(5);
  const [data, setData] = useState<ApiResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    const controller = new AbortController();

    async function load() {
      setLoading(true);
      setError("");
      setData(null);

      try {
        const params = new URLSearchParams({
          entityType,
          months: String(months),
          currency,
          endDate,
        });

        const response = await fetch(
          `/api/analytics/momentum?${params}`,
          { signal: controller.signal }
        );

        const result = await response.json();

        if (!response.ok) {
          throw new Error(result.error ?? "Request failed");
        }

        if (!controller.signal.aborted) {
          setData(result);
        }
      } catch (err) {
        if (!controller.signal.aborted) {
          setError(
            err instanceof Error ? err.message : String(err)
          );
        }
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      }
    }

    void load();
    return () => controller.abort();
  }, [entityType, months, currency, endDate]);

  const ranked = useMemo(() => {
    return (data?.rows ?? [])
      .filter(
        (row) =>
          row.current.runs >= minRuns &&
          row.previous.runs >= minRuns &&
          row.change[metric] !== null
      )
      .sort((a, b) => {
        const av = Number(a.change[metric]);
        const bv = Number(b.change[metric]);
        return direction === "HOT" ? bv - av : av - bv;
      });
  }, [data, minRuns, metric, direction]);

  const money = (value: number) =>
    new Intl.NumberFormat("en-GB", {
      style: "currency",
      currency,
      maximumFractionDigits: 0,
    }).format(value);

  const format = (value: number | null, selected: Metric) => {
    if (value === null) return "—";
    if (selected === "prizePerRun") return money(value);
    return value.toFixed(1) +
      (selected === "winRate" || selected === "placeRate" ? "%" : "");
  };

  const formatChange = (value: number | null) => {
    if (value === null) return "—";
    const sign = value > 0 ? "+" : "";
    if (metric === "prizePerRun") return sign + money(value);
    return sign + value.toFixed(1) +
      (metric === "winRate" || metric === "placeRate" ? " pp" : " pts");
  };

  return (
    <section className="mt-5">
      <h2 className="text-xl font-bold">
        Who&apos;s Hot? / Who&apos;s Cold?
      </h2>
      <p className="mt-1 text-sm text-gray-500">
        Compare performance across consecutive rolling periods.
        Minimum runs must be met in both periods.
      </p>

      <div className="mt-4 flex flex-wrap items-end gap-4 rounded-xl border p-4">
        <label className="flex flex-col gap-1 text-sm">
          Analyse
          <select
            value={entityType}
            onChange={(e) =>
              setEntityType(e.target.value as EntityType)
            }
            className="rounded border px-3 py-2"
          >
            <option value="HORSE">Horses</option>
            <option value="TRAINER">Trainers</option>
            <option value="JOCKEY">Jockeys</option>
          </select>
        </label>

        <label className="flex flex-col gap-1 text-sm">
          Rolling period
          <select
            value={months}
            onChange={(e) =>
              setMonths(Number(e.target.value) as 1 | 3 | 6)
            }
            className="rounded border px-3 py-2"
          >
            <option value={1}>1 month</option>
            <option value={3}>3 months</option>
            <option value={6}>6 months</option>
          </select>
        </label>

        <label className="flex flex-col gap-1 text-sm">
          Metric
          <select
            value={metric}
            onChange={(e) =>
              setMetric(e.target.value as Metric)
            }
            className="rounded border px-3 py-2"
          >
            {Object.entries(metrics).map(([key, label]) => (
              <option key={key} value={key}>
                {label}
              </option>
            ))}
          </select>
        </label>

        <label className="flex flex-col gap-1 text-sm">
          Minimum runs per period
          <input
            type="number"
            min={1}
            value={minRuns}
            onChange={(e) =>
              setMinRuns(Math.max(1, Number(e.target.value)))
            }
            className="w-28 rounded border px-3 py-2"
          />
        </label>

        <div className="flex gap-2">
          <button
            onClick={() => setDirection("HOT")}
            className={`rounded px-4 py-2 text-sm ${
              direction === "HOT"
                ? "bg-blue-700 text-white"
                : "bg-gray-100"
            }`}
          >
            Who&apos;s Hot?
          </button>
          <button
            onClick={() => setDirection("COLD")}
            className={`rounded px-4 py-2 text-sm ${
              direction === "COLD"
                ? "bg-blue-700 text-white"
                : "bg-gray-100"
            }`}
          >
            Who&apos;s Cold?
          </button>
        </div>
      </div>

      {loading && <p className="mt-4">Loading momentum...</p>}
      {error && <p className="mt-4 text-red-600">{error}</p>}

      {data && !loading && (
        <>
          <p className="mt-4 text-sm text-gray-500">
            Previous: {data.periods.previous.start} to{" "}
            {data.periods.previous.end}
            {" | "}
            Current: {data.periods.current.start} to{" "}
            {data.periods.current.end}
          </p>

          <div className="mt-4 overflow-x-auto rounded-xl border">
            <table className="w-full whitespace-nowrap text-sm">
              <thead className="bg-gray-100">
                <tr>
                  <th className="px-3 py-3 text-left">#</th>
                  <th className="px-3 py-3 text-left">
                    {entityType === "HORSE"
                      ? "Horse"
                      : entityType === "TRAINER"
                      ? "Trainer"
                      : "Jockey"}
                  </th>
                  <th className="px-3 py-3 text-right">
                    Previous runs
                  </th>
                  <th className="px-3 py-3 text-right">
                    Current runs
                  </th>
                  <th className="px-3 py-3 text-right">
                    Previous {metrics[metric]}
                  </th>
                  <th className="px-3 py-3 text-right">
                    Current {metrics[metric]}
                  </th>
                  <th className="px-3 py-3 text-right">
                    Change
                  </th>
                </tr>
              </thead>
              <tbody>
                {ranked.slice(0, 50).map((row, index) => (
                  <tr
                    key={row.name}
                    className="border-t hover:bg-gray-50"
                  >
                    <td className="px-3 py-3">{index + 1}</td>
                    <td className="px-3 py-3 font-semibold">
                      {row.name}
                    </td>
                    <td className="px-3 py-3 text-right">
                      {row.previous.runs}
                    </td>
                    <td className="px-3 py-3 text-right">
                      {row.current.runs}
                    </td>
                    <td className="px-3 py-3 text-right">
                      {format(row.previous[metric], metric)}
                    </td>
                    <td className="px-3 py-3 text-right">
                      {format(row.current[metric], metric)}
                    </td>
                    <td
                      className={`px-3 py-3 text-right font-semibold ${
                        Number(row.change[metric]) > 0
                          ? "text-green-700"
                          : Number(row.change[metric]) < 0
                          ? "text-red-700"
                          : ""
                      }`}
                    >
                      {formatChange(row.change[metric])}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <p className="mt-3 text-xs text-gray-500">
            Showing {Math.min(50, ranked.length)} of{" "}
            {ranked.length} qualifying entities.
            Win/place changes are percentage points;
            finish score changes are score points.
          </p>
        </>
      )}
    </section>
  );
}
