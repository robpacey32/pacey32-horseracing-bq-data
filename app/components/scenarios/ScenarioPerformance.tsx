
"use client";

import { useMemo, useState } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { ScenarioResult } from "@/lib/scenarios/types";

type ChartMetric = "cumulativeProfit" | "cumulativeROI" | "profit";

export interface PerformanceSelection {
  id: string;
  name: string;
  source: "modelled" | "saved" | "builder";
  conditions: string;
  startDate: string;
  endDate: string;
  result: ScenarioResult | null;
  testedStartDate?: string;
  testedEndDate?: string;
  runTimestamp?: string;
}

interface Props {
  // Existing interface, retained until page.tsx is updated.
  selection?: PerformanceSelection | null;

  // New multi-scenario interface.
  selections?: PerformanceSelection[];

  loading: boolean;
  error: string | null;
  onBacktest: (
    startDate: string,
    endDate: string
  ) => void | Promise<void>;

  // Optional controlled date range for the new page.
  startDate?: string;
  endDate?: string;
  onDateChange?: (
    startDate: string,
    endDate: string
  ) => void;
  onRemoveScenario?: (id: string) => void;
}

const COLORS = [
  "#e4c878",
  "#76c7c0",
  "#f3a683",
  "#b7a5e8",
  "#a9d18e",
  "#f0a6c1",
  "#90b4e8",
  "#e9b96e",
];

const OPTIONS: {
  key: ChartMetric;
  label: string;
}[] = [
  { key: "cumulativeProfit", label: "Cumulative Profit" },
  { key: "cumulativeROI", label: "Cumulative ROI" },
  { key: "profit", label: "Daily Profit" },
];

function money(value: number) {
  return value.toLocaleString("en-GB", {
    style: "currency",
    currency: "GBP",
  });
}

function percent(value: number) {
  return `${value.toFixed(2)}%`;
}

function shortDate(value: string) {
  if (!value) return "";

  const date = new Date(`${value.slice(0, 10)}T12:00:00Z`);

  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleDateString("en-GB", {
        day: "numeric",
        month: "short",
        year: "numeric",
      });
}

function compactMoney(value: number) {
  return value.toLocaleString("en-GB", {
    style: "currency",
    currency: "GBP",
    maximumFractionDigits: 2,
  });
}

function sourceLabel(source: PerformanceSelection["source"]) {
  if (source === "modelled") return "Modelled";
  if (source === "saved") return "Saved";
  return "Builder";
}

export default function ScenarioPerformance({
  selection,
  selections,
  loading,
  error,
  onBacktest,
  startDate: controlledStartDate,
  endDate: controlledEndDate,
  onDateChange,
  onRemoveScenario,
}: Props) {
  const [metric, setMetric] =
    useState<ChartMetric>("cumulativeProfit");

  const [localDates, setLocalDates] = useState<{
    id: string;
    startDate: string;
    endDate: string;
  } | null>(null);

  const activeSelections = useMemo(
    () =>
      selections ??
      (selection ? [selection] : []),
    [selections, selection]
  );

  const selectionKey = activeSelections
    .map((item) => item.id)
    .join("|");

  const fallbackStartDate =
    activeSelections[0]?.startDate ?? "";

  const fallbackEndDate =
    activeSelections[0]?.endDate ?? "";

  const startDate =
    controlledStartDate ??
    (localDates?.id === selectionKey
      ? localDates.startDate
      : fallbackStartDate);

  const endDate =
    controlledEndDate ??
    (localDates?.id === selectionKey
      ? localDates.endDate
      : fallbackEndDate);

  function updateDates(nextStart: string, nextEnd: string) {
    if (onDateChange) {
      onDateChange(nextStart, nextEnd);
    } else {
      setLocalDates({
        id: selectionKey,
        startDate: nextStart,
        endDate: nextEnd,
      });
    }
  }

  const completed = activeSelections.filter(
    (item) => item.result !== null
  );

  const canBacktest =
    activeSelections.length > 0 &&
    Boolean(startDate) &&
    Boolean(endDate) &&
    startDate <= endDate &&
    !loading;

  // Merge every scenario onto one date axis.
  // Carry forward cumulative values on days without bets.
  const chartData = useMemo(() => {
    const dates = new Set<string>();

    for (const scenario of completed) {
      for (const day of scenario.result?.dailyPerformance ?? []) {
        dates.add(day.date);
      }
    }

    const orderedDates = Array.from(dates).sort();

    const lookup = completed.map((scenario) => {
      const daily = [
        ...(scenario.result?.dailyPerformance ?? []),
      ].sort((a, b) => a.date.localeCompare(b.date));

      return {
        id: scenario.id,
        daily,
      };
    });

    const positions = lookup.map(() => 0);
    const cumulativeProfit = lookup.map(() => 0);
    const cumulativeROI = lookup.map(() => 0);

    return orderedDates.map((date) => {
      const row: Record<string, string | number> = { date };

      lookup.forEach((scenario, index) => {
        let dailyProfit = 0;

        while (
          positions[index] < scenario.daily.length &&
          scenario.daily[positions[index]].date <= date
        ) {
          const point = scenario.daily[positions[index]];

          cumulativeProfit[index] = point.cumulativeProfit;
          cumulativeROI[index] = point.cumulativeROI;

          if (point.date === date) {
            dailyProfit += point.profit;
          }

          positions[index]++;
        }

        row[scenario.id] =
          metric === "cumulativeProfit"
            ? cumulativeProfit[index]
            : metric === "cumulativeROI"
              ? cumulativeROI[index]
              : dailyProfit;
      });

      return row;
    });
  }, [completed, metric]);

  const formatChartValue = (value: number) =>
    metric === "cumulativeROI"
      ? percent(value)
      : money(value);

  return (
    <section className="overflow-hidden rounded-xl border border-[#ded8c9] bg-white shadow-sm">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-[#ded8c9] bg-[#f8f7f2] px-5 py-4">
        <h2 className="font-serif text-2xl text-[#123d2d]">
          Scenario Performance
        </h2>

        <span className="text-sm text-gray-500">
          {activeSelections.length} selected
        </span>
      </div>

      <div className="space-y-4 p-4 md:p-5">
        <div className="flex flex-wrap items-end gap-3 rounded-lg bg-[#f8f7f2] p-3">
          <label className="text-xs font-medium text-gray-700">
            From
            <input
              type="date"
              value={startDate}
              onChange={(event) =>
                updateDates(event.target.value, endDate)
              }
              className="mt-1 block w-[155px] rounded-md border border-gray-300 bg-white px-2 py-1.5 text-sm"
            />
          </label>

          <label className="text-xs font-medium text-gray-700">
            To
            <input
              type="date"
              value={endDate}
              onChange={(event) =>
                updateDates(startDate, event.target.value)
              }
              className="mt-1 block w-[155px] rounded-md border border-gray-300 bg-white px-2 py-1.5 text-sm"
            />
          </label>

          <button
            type="button"
            disabled={!canBacktest}
            onClick={() =>
              void onBacktest(startDate, endDate)
            }
            className="rounded-md bg-[#123d2d] px-4 py-2 text-sm font-semibold text-white disabled:opacity-50"
          >
            {loading
              ? "Calculating…"
              : activeSelections.length > 1
                ? `Backtest ${activeSelections.length} Scenarios`
                : "Run Backtest"}
          </button>

          <p className="text-xs text-gray-500">
            All selected scenarios use the same date range.
          </p>
        </div>

        {error && (
          <p role="alert" className="text-sm text-red-700">
            {error}
          </p>
        )}

        {activeSelections.length === 0 ? (
          <p className="py-5 text-sm text-gray-500">
            Select scenarios above to compare their performance.
          </p>
        ) : (
          <div className="overflow-x-auto rounded-lg border border-[#ded8c9]">
            <table className="w-full min-w-[1000px] text-sm">
              <thead className="bg-[#f8f7f2] text-xs uppercase tracking-wide text-gray-600">
                <tr>
                  <th className="px-3 py-3 text-left">
                    Scenario
                  </th>
                  <th className="w-10 px-2 py-3 text-center">Remove</th>
                  <th className="px-3 py-3 text-right">Bets</th>
                  <th className="px-3 py-3 text-right">Winners</th>
                  <th className="px-3 py-3 text-right">Strike Rate</th>
                  <th className="px-3 py-3 text-right">ROI</th>
                  <th className="px-3 py-3 text-right">Staked</th>
                  <th className="px-3 py-3 text-right">Returns</th>
                  <th className="px-3 py-3 text-right">Profit</th>
                  <th className="px-3 py-3 text-right">Max DD</th>
                </tr>
              </thead>

              <tbody className="divide-y divide-[#e8e2d6]">
                {activeSelections.map((item, index) => {
                  const summary = item.result?.summary;
                  const color = COLORS[index % COLORS.length];

                  return (
                    <tr key={item.id} className="hover:bg-[#fffdf7]">
                      <td className="px-2 py-3 text-center">
                        <button
                          type="button"
                          onClick={() => onRemoveScenario?.(item.id)}
                          disabled={!onRemoveScenario}
                          title={`Remove ${item.name} from comparison`}
                          aria-label={`Remove ${item.name} from comparison`}
                          className="inline-flex h-7 w-7 items-center justify-center rounded-md border border-gray-200 text-lg leading-none text-gray-500 hover:border-red-200 hover:bg-red-50 hover:text-red-700 focus-visible:outline focus-visible:outline-2 focus-visible:outline-[#123d2d] disabled:opacity-40"
                        >
                          ×
                        </button>
                      </td>
                      <td className="px-3 py-3">
                        <div className="flex items-start gap-2">
                          <span
                            className="mt-1 h-3 w-3 shrink-0 rounded-full"
                            style={{ backgroundColor: color }}
                          />

                          <div className="min-w-[150px]">
                            <div className="font-semibold text-[#123d2d]">
                              {item.name}
                            </div>
                            <div className="text-xs text-gray-500">
                              {sourceLabel(item.source)}
                            </div>
                            {item.conditions && (
                              <div
                                className="mt-1 max-w-[230px] truncate text-xs text-gray-500"
                                title={item.conditions}
                              >
                                {item.conditions}
                              </div>
                            )}
                            {item.result && (
                              <div className="mt-1 text-xs text-gray-500">
                                {shortDate(
                                  item.testedStartDate ?? item.startDate
                                )}{" "}
                                –{" "}
                                {shortDate(
                                  item.testedEndDate ?? item.endDate
                                )}
                              </div>
                            )}
                          </div>
                        </div>
                      </td>

                      {summary ? (
                        <>
                          <td className="px-3 py-3 text-right tabular-nums">
                            {summary.bets.toLocaleString("en-GB")}
                          </td>
                          <td className="px-3 py-3 text-right tabular-nums">
                            {summary.winners.toLocaleString("en-GB")}
                          </td>
                          <td className="px-3 py-3 text-right tabular-nums">
                            {percent(summary.strikeRate)}
                          </td>
                          <td
                            className={`px-3 py-3 text-right font-semibold tabular-nums ${
                              summary.roi < 0 ? "text-red-700" : "text-[#123d2d]"
                            }`}
                          >
                            {percent(summary.roi)}
                          </td>
                          <td className="px-3 py-3 text-right tabular-nums">
                            {compactMoney(summary.staked)}
                          </td>
                          <td className="px-3 py-3 text-right tabular-nums">
                            {compactMoney(summary.returns)}
                          </td>
                          <td
                            className={`px-3 py-3 text-right font-semibold tabular-nums ${
                              summary.profit < 0
                                ? "text-red-700"
                                : "text-[#123d2d]"
                            }`}
                          >
                            {compactMoney(summary.profit)}
                          </td>
                          <td className="px-3 py-3 text-right tabular-nums">
                            {compactMoney(summary.maxDrawdown)}
                          </td>
                        </>
                      ) : (
                        <td
                          colSpan={8}
                          className="px-3 py-3 text-center text-xs text-gray-500"
                        >
                          {loading
                            ? "Calculating…"
                            : "Awaiting backtest"}
                        </td>
                      )}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        <div className="rounded-xl bg-[#123d2d] p-4 text-white md:p-5">
          <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
            <h3 className="font-serif text-xl">
              Performance Over Time
            </h3>

            <div className="flex flex-wrap gap-2">
              {OPTIONS.map((option) => (
                <button
                  key={option.key}
                  type="button"
                  onClick={() => setMetric(option.key)}
                  aria-pressed={metric === option.key}
                  className={`rounded-md px-3 py-1.5 text-xs font-medium ${
                    metric === option.key
                      ? "bg-white text-[#123d2d]"
                      : "border border-white/30 text-white"
                  }`}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </div>

          {chartData.length === 0 ? (
            <div className="flex h-[220px] items-center justify-center text-center text-sm text-white/70">
              {loading
                ? "Calculating historical performance…"
                : "Run a backtest to display performance."}
            </div>
          ) : (
            <div className="h-[340px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData}>
                  <CartesianGrid
                    stroke="rgba(255,255,255,0.12)"
                    strokeDasharray="3 3"
                  />

                  <XAxis
                    dataKey="date"
                    tickFormatter={(value: string) =>
                      shortDate(value).replace(/ \d{4}$/, "")
                    }
                    stroke="#e5e7eb"
                    tick={{ fontSize: 11 }}
                    minTickGap={35}
                  />

                  <YAxis
                    stroke="#e5e7eb"
                    tick={{ fontSize: 11 }}
                    width={75}
                    tickFormatter={(value: number) =>
                      metric === "cumulativeROI"
                        ? `${value}%`
                        : `£${value}`
                    }
                  />

                  <ReferenceLine
                    y={0}
                    stroke="rgba(255,255,255,0.5)"
                    strokeDasharray="4 4"
                  />

                  <Tooltip
                    labelFormatter={(value) =>
                      shortDate(String(value))
                    }
                    formatter={(value, name) => [
                      formatChartValue(Number(value ?? 0)),
                      String(name),
                    ]}
                    contentStyle={{
                      background: "#fffdf7",
                      color: "#202820",
                      border: "1px solid #ded8c9",
                      borderRadius: 8,
                    }}
                  />

                  <Legend
                    wrapperStyle={{
                      fontSize: 12,
                      paddingTop: 8,
                    }}
                  />

                  {completed.map((item) => {
                    const index = activeSelections.findIndex(
                      (selected) => selected.id === item.id
                    );

                    return (
                      <Line
                        key={item.id}
                        type="linear"
                        dataKey={item.id}
                        name={item.name}
                        stroke={COLORS[index % COLORS.length]}
                        strokeWidth={2.5}
                        dot={false}
                        activeDot={{ r: 4 }}
                        connectNulls
                        isAnimationActive={false}
                      />
                    );
                  })}
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>

        <p className="text-xs text-gray-500">
          Modelled scenarios retain their original one-horse-per-race
          selection rules. Custom and saved scenarios may select
          multiple horses per race. Historical backtests are separate
          from model discovery-period statistics.
        </p>
      </div>
    </section>
  );
}
