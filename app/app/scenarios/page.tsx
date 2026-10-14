"use client";

import { useEffect, useState } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type ChartMetric = "cumulativeProfit" | "cumulativeROI" | "profit";

interface DailyPerformance {
  date: string;
  bets: number;
  winners: number;
  staked: number;
  returns: number;
  profit: number;
  cumulativeProfit: number;
  cumulativeROI: number;
}

interface ScenarioSummary {
  bets: number;
  winners: number;
  strikeRate: number;
  staked: number;
  returns: number;
  profit: number;
  roi: number;
  maxDrawdown: number;
}

interface ScenarioResult {
  summary: ScenarioSummary;
  dailyPerformance: DailyPerformance[];
}

function formatNumber(value: number, decimals = 2) {
  return value.toLocaleString("en-GB", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function MetricCard({
  label,
  value,
  negative = false,
}: {
  label: string;
  value: string;
  negative?: boolean;
}) {
  return (
    <div
      className="rounded-xl border p-5"
      style={{
        background: "var(--racing-ivory, #fffdf7)",
        borderColor: "var(--racing-border, #ded8c9)",
      }}
    >
      <div className="text-xs font-semibold uppercase tracking-widest text-gray-500">
        {label}
      </div>

      <div
        className={`mt-3 text-2xl font-semibold ${
          negative ? "text-red-700" : ""
        }`}
        style={
          negative
            ? undefined
            : { color: "var(--racing-green, #164734)" }
        }
      >
        {value}
      </div>
    </div>
  );
}

export default function ScenariosPage() {
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");

  useEffect(() => {
    const today = new Date();

    const start = new Date(today);
    start.setMonth(start.getMonth() - 3);

    setStartDate(start.toISOString().slice(0, 10));
    setEndDate(today.toISOString().slice(0, 10));
  }, []);

  const [stake, setStake] = useState(10);
  const [betType, setBetType] = useState<"win" | "each_way">("win");
  const [stakeAllocation, setStakeAllocation] = useState<
    "per_horse" | "per_race"
  >("per_horse");
  const [stakeMode, setStakeMode] = useState<"total" | "per_leg">(
    "total"
  );

  const [minOdds, setMinOdds] = useState("2");
  const [maxOdds, setMaxOdds] = useState("10");
  const [maxPreRaceRank, setMaxPreRaceRank] = useState("3");
  const [raceType, setRaceType] = useState("Flat");

  const [chartMetric, setChartMetric] =
    useState<ChartMetric>("cumulativeProfit");

  const [result, setResult] = useState<ScenarioResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function runScenario() {
    setLoading(true);
    setError(null);

    try {
      const filters: Record<string, string | number> = {};

      if (minOdds !== "") filters.minOdds = Number(minOdds);
      if (maxOdds !== "") filters.maxOdds = Number(maxOdds);

      if (maxPreRaceRank !== "") {
        filters.maxPreRaceRank = Number(maxPreRaceRank);
      }

      if (raceType !== "All") {
        filters.raceType = raceType;
      }

      const response = await fetch("/api/scenarios/calculate", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          startDate,
          endDate,
          stake,
          betType,
          stakeAllocation,
          stakeMode,
          filters,
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || "Scenario calculation failed.");
      }

      setResult(data);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "An unexpected error occurred."
      );
    } finally {
      setLoading(false);
    }
  }

  const chartOptions: {
    key: ChartMetric;
    label: string;
  }[] = [
    { key: "cumulativeProfit", label: "Cumulative Profit" },
    { key: "cumulativeROI", label: "Cumulative ROI" },
    { key: "profit", label: "Daily Profit" },
  ];

  const chartIsPercent = chartMetric === "cumulativeROI";

  return (
    <main
      className="min-h-screen px-5 py-8 md:px-8"
      style={{
        background: "var(--racing-paper, #f7f4ed)",
        color: "var(--racing-ink, #202820)",
      }}
    >
      <div className="mx-auto max-w-7xl">
        {/* Heading */}
        <div className="mb-8">
          <p
            className="text-xs font-semibold uppercase tracking-[0.25em]"
            style={{ color: "var(--racing-gold-dark, #9c792e)" }}
          >
            Pacey32 Analytics
          </p>

          <h1
            className="mt-2 font-serif text-4xl font-semibold"
            style={{ color: "var(--racing-green-dark, #123d2d)" }}
          >
            Scenarios
          </h1>

          <p className="mt-3 text-sm text-gray-600">
            Build and evaluate historical horse racing betting strategies.
          </p>
        </div>

        {/* Scenario Builder */}
        <section
          className="mb-7 rounded-xl border bg-white p-6 shadow-sm"
          style={{
            borderColor: "var(--racing-border, #ded8c9)",
          }}
        >
          <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
            <h2
              className="font-serif text-2xl"
              style={{ color: "var(--racing-green-dark, #123d2d)" }}
            >
              Scenario Builder
            </h2>

            <span className="text-xs text-gray-500">
              Historical strategy testing
            </span>
          </div>

          <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-4">
            <label className="text-sm">
              <span className="mb-2 block font-medium">Start date</span>
              <input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              />
            </label>

            <label className="text-sm">
              <span className="mb-2 block font-medium">End date</span>
              <input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              />
            </label>

            <label className="text-sm">
              <span className="mb-2 block font-medium">Stake</span>
              <input
                type="number"
                min="0.01"
                step="0.5"
                value={stake}
                onChange={(e) => setStake(Number(e.target.value))}
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              />
            </label>

            <label className="text-sm">
              <span className="mb-2 block font-medium">Bet type</span>
              <select
                value={betType}
                onChange={(e) =>
                  setBetType(e.target.value as "win" | "each_way")
                }
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              >
                <option value="win">Win</option>
                <option value="each_way">Each-way</option>
              </select>
            </label>

            <label className="text-sm">
              <span className="mb-2 block font-medium">
                Stake allocation
              </span>
              <select
                value={stakeAllocation}
                onChange={(e) =>
                  setStakeAllocation(
                    e.target.value as "per_horse" | "per_race"
                  )
                }
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              >
                <option value="per_horse">Per horse</option>
                <option value="per_race">Per race</option>
              </select>
            </label>

            <label className="text-sm">
              <span className="mb-2 block font-medium">Stake mode</span>
              <select
                value={stakeMode}
                onChange={(e) =>
                  setStakeMode(e.target.value as "total" | "per_leg")
                }
                disabled={betType === "win"}
                className="w-full rounded-lg border border-gray-300 px-3 py-2 disabled:opacity-50"
              >
                <option value="total">Total stake</option>
                <option value="per_leg">Per leg</option>
              </select>
            </label>

            <label className="text-sm">
              <span className="mb-2 block font-medium">Minimum odds</span>
              <input
                type="number"
                min="1"
                step="0.1"
                value={minOdds}
                onChange={(e) => setMinOdds(e.target.value)}
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              />
            </label>

            <label className="text-sm">
              <span className="mb-2 block font-medium">Maximum odds</span>
              <input
                type="number"
                min="1"
                step="0.1"
                value={maxOdds}
                onChange={(e) => setMaxOdds(e.target.value)}
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              />
            </label>

            <label className="text-sm">
              <span className="mb-2 block font-medium">
                Maximum odds rank
              </span>
              <input
                type="number"
                min="1"
                step="1"
                value={maxPreRaceRank}
                onChange={(e) => setMaxPreRaceRank(e.target.value)}
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              />
            </label>

            <label className="text-sm">
              <span className="mb-2 block font-medium">Race type</span>
              <select
                value={raceType}
                onChange={(e) => setRaceType(e.target.value)}
                className="w-full rounded-lg border border-gray-300 px-3 py-2"
              >
                <option value="All">All</option>
                <option value="Flat">Flat</option>
                <option value="Jumps">Jumps</option>
                <option value="Unknown">Unknown</option>
              </select>
            </label>
          </div>

          <div className="mt-6 flex justify-end">
            <button
              onClick={runScenario}
              disabled={loading || !startDate || !endDate}
              className="rounded-lg px-6 py-3 text-sm font-semibold text-white transition hover:opacity-90 disabled:opacity-50"
              style={{
                background: "var(--racing-green, #164734)",
              }}
            >
              {loading ? "Calculating..." : "Run Scenario"}
            </button>
          </div>

          {error && (
            <p className="mt-4 text-sm text-red-700">{error}</p>
          )}
        </section>

        {/* Performance Chart */}
        <section
          className="mb-7 overflow-hidden rounded-xl p-6 text-white shadow-lg"
          style={{
            background: "var(--racing-green-dark, #123d2d)",
          }}
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
              {chartOptions.map((option) => (
                <button
                  key={option.key}
                  onClick={() => setChartMetric(option.key)}
                  className={`rounded-lg px-3 py-2 text-xs font-medium transition ${
                    chartMetric === option.key
                      ? "bg-white text-green-950"
                      : "border border-white/30 text-white hover:bg-white/10"
                  }`}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </div>

          {result && result.dailyPerformance.length > 0 ? (
            <div className="h-[360px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart
                  data={result.dailyPerformance}
                  margin={{
                    top: 10,
                    right: 20,
                    left: 10,
                    bottom: 10,
                  }}
                >
                  <CartesianGrid
                    stroke="rgba(255,255,255,0.12)"
                    strokeDasharray="3 3"
                  />

                  <XAxis
                    dataKey="date"
                    stroke="#e5e7eb"
                    tick={{ fontSize: 11 }}
                    minTickGap={35}
                  />

                  <YAxis
                    stroke="#e5e7eb"
                    tick={{ fontSize: 11 }}
                    width={75}
                    tickFormatter={(value: number) =>
                      chartIsPercent
                        ? `${formatNumber(value, 0)}%`
                        : formatNumber(value, 0)
                    }
                  />

                  <Tooltip
                    contentStyle={{
                      background: "#fffdf7",
                      color: "#202820",
                      border: "1px solid #ded8c9",
                      borderRadius: 8,
                    }}
                    formatter={(value, name) => [
                      `${
                        chartIsPercent ? "" : ""
                      }${formatNumber(Number(value))}${
                        chartIsPercent ? "%" : ""
                      }`,
                      String(name),
                    ]}
                  />

                  <Line
                    type="monotone"
                    dataKey={chartMetric}
                    name={
                      chartOptions.find(
                        (option) => option.key === chartMetric
                      )?.label
                    }
                    stroke="#e4c878"
                    strokeWidth={2.5}
                    dot={false}
                    activeDot={{ r: 5 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="flex h-[360px] items-center justify-center rounded-lg border border-dashed border-white/25 text-center text-sm text-white/70">
              {loading
                ? "Calculating historical performance..."
                : result
                  ? "No qualifying bets found for this scenario."
                  : "Configure your scenario and select Run Scenario to view performance."}
            </div>
          )}
        </section>

        {/* Summary */}
        {result && (
          <section className="mb-7">
            <h2
              className="mb-4 font-serif text-2xl"
              style={{
                color: "var(--racing-green-dark, #123d2d)",
              }}
            >
              Performance Summary
            </h2>

            <div className="grid grid-cols-2 gap-4 md:grid-cols-3 lg:grid-cols-6">
              <MetricCard
                label="Bets"
                value={formatNumber(result.summary.bets, 0)}
              />

              <MetricCard
                label="Strike Rate"
                value={`${formatNumber(result.summary.strikeRate)}%`}
              />

              <MetricCard
                label="Staked"
                value={formatNumber(result.summary.staked)}
              />

              <MetricCard
                label="Profit"
                value={formatNumber(result.summary.profit)}
                negative={result.summary.profit < 0}
              />

              <MetricCard
                label="ROI"
                value={`${formatNumber(result.summary.roi)}%`}
                negative={result.summary.roi < 0}
              />

              <MetricCard
                label="Max Drawdown"
                value={formatNumber(result.summary.maxDrawdown)}
              />
            </div>
          </section>
        )}
      </div>
    </main>
  );
}