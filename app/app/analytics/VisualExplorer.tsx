
"use client";

import { useMemo, useState } from "react";
import {
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";

type Metric =
  | "winRate"
  | "placeRate"
  | "averageFinishPercentile"
  | "prizePerRun"
  | "prizeMoney";

export type ExplorerRow = {
  name: string;
  runs: number;
  wins: number;
  winRate: number;
  placeRate: number;
  averageFinishPercentile: number | null;
  prizeMoney: number;
  prizePerRun: number;
};

type Props = {
  rows: ExplorerRow[];
  currency: "GBP" | "EUR";
};

const metrics: Record<Metric, string> = {
  winRate: "Win %",
  placeRate: "Place %",
  averageFinishPercentile: "Finish score",
  prizePerRun: "Prize / run",
  prizeMoney: "Total prize money",
};

function median(values: number[]) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2
    ? sorted[middle]
    : (sorted[middle - 1] + sorted[middle]) / 2;
}

export default function VisualExplorer({ rows, currency }: Props) {
  const [xMetric, setXMetric] = useState<Metric>("winRate");
  const [yMetric, setYMetric] = useState<Metric>("prizePerRun");

  const points = useMemo(
    () =>
      rows
        .filter(
          (row) =>
            row[xMetric] !== null &&
            row[yMetric] !== null
        )
        .map((row) => ({
          ...row,
          x: Number(row[xMetric]),
          y: Number(row[yMetric]),
        })),
    [rows, xMetric, yMetric]
  );

  const xMedian = median(points.map((point) => point.x));
  const yMedian = median(points.map((point) => point.y));

  const formatValue = (value: number, metric: Metric) => {
    if (metric === "prizePerRun" || metric === "prizeMoney") {
      return new Intl.NumberFormat("en-GB", {
        style: "currency",
        currency,
        maximumFractionDigits: 0,
      }).format(value);
    }

    return `${value.toFixed(1)}${
      metric === "winRate" || metric === "placeRate" ? "%" : ""
    }`;
  };

  return (
    <section className="mt-5 rounded-xl border p-4">
      <h2 className="text-xl font-bold">Visual Explorer</h2>
      <p className="mt-1 text-sm text-gray-500">
        Compare two performance metrics. Top-right indicates
        higher values on both axes.
      </p>

      <div className="mt-4 flex flex-wrap gap-4">
        <label className="flex flex-col gap-1 text-sm">
          X-axis
          <select
            value={xMetric}
            onChange={(event) =>
              setXMetric(event.target.value as Metric)
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
          Y-axis
          <select
            value={yMetric}
            onChange={(event) =>
              setYMetric(event.target.value as Metric)
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
      </div>

      <div className="mt-5 h-[480px] w-full">
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 20, right: 30, bottom: 30, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              type="number"
              dataKey="x"
              name={metrics[xMetric]}
              tickFormatter={(value: number) =>
                formatValue(value, xMetric)
              }
              domain={["auto", "auto"]}
            />
            <YAxis
              type="number"
              dataKey="y"
              name={metrics[yMetric]}
              tickFormatter={(value: number) =>
                formatValue(value, yMetric)
              }
              width={100}
              domain={["auto", "auto"]}
            />
            <ZAxis range={[45, 45]} />
            <Tooltip
              cursor={{ strokeDasharray: "3 3" }}
              content={({ active, payload }) => {
                if (!active || !payload?.length) return null;
                const point = payload[0].payload as ExplorerRow & {
                  x: number;
                  y: number;
                };

                return (
                  <div className="rounded-lg border bg-white p-3 text-sm shadow">
                    <p className="font-bold">{point.name}</p>
                    <p>Runs: {point.runs}</p>
                    <p>
                      {metrics[xMetric]}:{" "}
                      {formatValue(point.x, xMetric)}
                    </p>
                    <p>
                      {metrics[yMetric]}:{" "}
                      {formatValue(point.y, yMetric)}
                    </p>
                  </div>
                );
              }}
            />
            <ReferenceLine x={xMedian} stroke="#64748b" strokeDasharray="5 5" />
            <ReferenceLine y={yMedian} stroke="#64748b" strokeDasharray="5 5" />
            <Scatter data={points} fill="#2563eb" />
          </ScatterChart>
        </ResponsiveContainer>
      </div>

      <p className="mt-2 text-xs text-gray-500">
        {points.length} entities plotted. Dashed lines show
        median values for each axis.
      </p>
    </section>
  );
}
