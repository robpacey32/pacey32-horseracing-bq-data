"use client";

import { useMemo, useState } from "react";

import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { ComparedScenario } from "@/lib/scenarios/types";

type ChartMetric = "cumulativeProfit" | "cumulativeROI" | "profit";

const COLOURS = [
  "#e4c878", // Racing gold
  "#77b7a0", // Sage
  "#a7b9df", // Blue
  "#df9c83", // Copper
];

function formatDate(value: string) {
  return new Date(`${value}T12:00:00Z`).toLocaleDateString("en-GB", {
    day: "numeric",
    month: "short",
  });
}

function formatNumber(value: number) {
  return value.toLocaleString("en-GB", {
    maximumFractionDigits: 2,
  });
}

interface Props {
  scenarios: ComparedScenario[];
}

export default function ScenarioChart({ scenarios }: Props) {
  const [metric, setMetric] =
    useState<ChartMetric>("cumulativeProfit");

  const chartData = useMemo(() => {
    if (scenarios.length === 0) return [];

    const allDates = new Set<string>();

    for (const scenario of scenarios) {
      allDates.add(scenario.definition.startDate);

      for (const day of scenario.result.dailyPerformance) {
        allDates.add(day.date);
      }
    }

    const dates = [...allDates].sort();

    const indexes = scenarios.map((scenario) => {
      return new Map(
        scenario.result.dailyPerformance.map((day) => [
          day.date,
          day,
        ])
      );
    });

    const previous = scenarios.map(() => ({
      cumulativeProfit: 0,
      cumulativeROI: 0,
    }));

    return dates.map((date) => {
      const row: Record<string, string | number> = {
        date,
      };

      scenarios.forEach((scenario, index) => {
        const day = indexes[index].get(date);

        if (day) {
          previous[index] = {
            cumulativeProfit: day.cumulativeProfit,
            cumulativeROI: day.cumulativeROI,
          };
        }

        row[`scenario_${index}`] =
          metric === "profit"
            ? day?.profit ?? 0
            : previous[index][metric];
      });

      return row;
    });
  }, [scenarios, metric]);

  const options: Array<{ key: ChartMetric; label: string }> = [
    { key: "cumulativeProfit", label: "Cumulative Profit" },
    { key: "cumulativeROI", label: "Cumulative ROI" },
    { key: "profit", label: "Daily Profit" },
  ];

  return (
    <section
      className="mb-7 rounded-xl p-6 text-white shadow-lg"
      style={{ background: "var(--racing-green-dark, #123d2d)" }}
    >
      <div className="mb-6 flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-xs uppercase tracking-widest text-amber-200">
            Performance analysis
          </p>

          <h2 className="mt-2 font-serif text-2xl">
            Performance Over Time
          </h2>
        </div>

        <div className="flex flex-wrap gap-2">
          {options.map((option) => (
            <button
              key={option.key}
              onClick={() => setMetric(option.key)}
              className={`rounded-lg px-3 py-2 text-xs ${
                metric === option.key
                  ? "bg-white text-green-950"
                  : "border border-white/30 text-white"
              }`}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>

      {scenarios.length === 0 ? (
        <div className="flex h-[360px] items-center justify-center text-white/70">
          Run a scenario to display its historical performance.
        </div>
      ) : (
        <>
          <div className="mb-5 flex flex-wrap gap-5">
            {scenarios.map((scenario, index) => (
              <div
                key={scenario.definition.id}
                className="flex items-center gap-2 text-sm"
              >
                <span
                  className="h-3 w-3 rounded-full"
                  style={{ background: COLOURS[index] }}
                />

                {scenario.definition.name}
              </div>
            ))}
          </div>

          <div className="h-[380px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid
                  stroke="rgba(255,255,255,0.12)"
                  strokeDasharray="3 3"
                />

                <XAxis
                  dataKey="date"
                  tickFormatter={formatDate}
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
                      ? `${formatNumber(value)}%`
                      : formatNumber(value)
                  }
                />

                <ReferenceLine
                  y={0}
                  stroke="rgba(255,255,255,0.55)"
                  strokeDasharray="4 4"
                />

                <Tooltip
                  labelFormatter={(label) =>
                    new Date(
                      `${String(label)}T12:00:00Z`
                    ).toLocaleDateString("en-GB", {
                      day: "numeric",
                      month: "long",
                      year: "numeric",
                    })
                  }
                  formatter={(value, name) => [
                    `${formatNumber(Number(value))}${
                      metric === "cumulativeROI" ? "%" : ""
                    }`,
                    String(name),
                  ]}
                  contentStyle={{
                    background: "#fffdf7",
                    color: "#202820",
                    border: "1px solid #ded8c9",
                    borderRadius: 8,
                  }}
                />

                {scenarios.map((scenario, index) => (
                  <Line
                    key={scenario.definition.id}
                    type="linear"
                    dataKey={`scenario_${index}`}
                    name={scenario.definition.name}
                    stroke={COLOURS[index]}
                    strokeWidth={2.5}
                    dot={false}
                    activeDot={{ r: 5 }}
                    isAnimationActive={false}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </>
      )}
    </section>
  );
}