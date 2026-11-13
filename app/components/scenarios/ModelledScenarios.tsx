
"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

export type ModelStrategy = {
  runTimestamp: string;
  lookbackMonths: number;
  rank: number;
  conditions: string;
  bets: number;
  betsPerWeek: number;
  activeDays: number;
  profit: number;
  profitPerBet: number;
  profitPerDay: number;
  modelRoi: number;
  strikeRate: number;
};

type ModelRun = {
  timestamp: string;
  strategies: number;
};

type ResponseData = {
  runs: ModelRun[];
  selectedRun: string | null;
  strategies: ModelStrategy[];
  error?: string;
};

type SortKey =
  | "rank"
  | "bets"
  | "betsPerWeek"
  | "profit"
  | "profitPerBet"
  | "strikeRate";

interface Props {
  // Existing single-selection interface.
  selectedId?: string | null;
  onSelect?: (strategy: ModelStrategy) => void;

  // New multi-selection interface.
  selectedIds?: string[];
  onToggle?: (strategy: ModelStrategy) => void;

  isAdmin?: boolean;
}

export function modelStrategyId(strategy: ModelStrategy): string {
  return `modelled:${strategy.runTimestamp}:${strategy.rank}`;
}

const money = (value: number) =>
  value.toLocaleString("en-GB", {
    style: "currency",
    currency: "GBP",
  });

const modelPercent = (value: number) =>
  `${(value * 100).toFixed(1)}%`;

const dateLabel = (value: string) =>
  new Date(value).toLocaleString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });

export function formatModelConditions(conditions: string): string {
  const parts = conditions
    .split(/\s*(?:&&|&|\bAND\b)\s*/i)
    .map((part) =>
      part.trim().replace(/^\(+|\)+$/g, "")
    )
    .filter(Boolean);

  const labels: Record<string, string> = {
    BF: "BF",
    C: "C",
    D: "D",
    CD: "CD",
    GOING_STANDARD: "Standard Going",
  };

  function translate(expression: string): string {
    const raw = expression.trim();

    const booleanMatch = raw.match(
      /^(BF|C|D|CD|Going_Standard)\s*(==|=|!=|<>)\s*([01])$/i
    );

    if (booleanMatch) {
      const key = booleanMatch[1].toUpperCase();
      const operator = booleanMatch[2];
      const value = Number(booleanMatch[3]);

      const enabled =
        operator === "!=" || operator === "<>"
          ? value === 0
          : value === 1;

      return enabled
        ? labels[key]
        : `Exclude ${labels[key]}`;
    }

    if (/^(BF|C|D|CD|Going_Standard)$/i.test(raw)) {
      return labels[raw.toUpperCase()];
    }

    const rankMatch = raw.match(
      /^PreRaceRank\s*(<=|>=|<|>|==|=)\s*(\d+)$/i
    );

    if (rankMatch) {
      const operator = rankMatch[1];
      const rank = Number(rankMatch[2]);

      if (operator === "<=") return `Top ${rank} in odds`;
      if (operator === "<") return `Top ${rank - 1} in odds`;
      if (operator === ">=") return `Odds rank ${rank} or higher`;
      if (operator === ">") return `Odds rank above ${rank}`;

      return `Odds rank ${rank}`;
    }

    const oddsMatch = raw.match(
      /^(Odds|Odds_Decimal|odds_dec)\s*(<=|>=|<|>|==|=)\s*(\d+(?:\.\d+)?)$/i
    );

    if (oddsMatch) {
      const operator = oddsMatch[2];
      const value = oddsMatch[3];

      const description: Record<string, string> = {
        "<=": "at most",
        ">=": "at least",
        "<": "below",
        ">": "above",
        "==": "equal to",
        "=": "equal to",
      };

      return `Decimal odds ${description[operator]} ${value}`;
    }

    return raw.replace(/_/g, " ");
  }

  return parts.map(translate).join(", ");
}

export default function ModelledScenarios({
  selectedId = null,
  onSelect,
  selectedIds,
  onToggle,
  isAdmin = false,
}: Props) {
  const [data, setData] = useState<ResponseData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [localSelectedIds, setLocalSelectedIds] =
    useState<string[]>([]);

  const [limit, setLimit] = useState(5);
  const [sortKey, setSortKey] = useState<SortKey>("rank");
  const [sortDirection, setSortDirection] =
    useState<"asc" | "desc">("asc");

  const activeIds =
    selectedIds ??
    (onSelect
      ? selectedId
        ? [selectedId]
        : []
      : localSelectedIds);

  const load = useCallback(async (run?: string) => {
    setLoading(true);
    setError(null);

    try {
      const url = run
        ? `/api/scenarios/modelled?run=${encodeURIComponent(run)}`
        : "/api/scenarios/modelled";

      const response = await fetch(url, {
        cache: "no-store",
      });

      const json = (await response.json()) as ResponseData;

      if (!response.ok) {
        throw new Error(
          json.error || "Unable to load modelled scenarios."
        );
      }

      setData(json);
      setLocalSelectedIds([]);
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "Unable to load modelled scenarios."
      );
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const sorted = useMemo(() => {
    return [...(data?.strategies ?? [])].sort((a, b) => {
      const difference = a[sortKey] - b[sortKey];

      return sortDirection === "asc"
        ? difference
        : -difference;
    });
  }, [data, sortKey, sortDirection]);

  function changeSort(key: SortKey) {
    if (key === sortKey) {
      setSortDirection((previous) =>
        previous === "asc" ? "desc" : "asc"
      );
    } else {
      setSortKey(key);
      setSortDirection(key === "rank" ? "asc" : "desc");
    }
  }

  function toggleStrategy(strategy: ModelStrategy) {
    const id = modelStrategyId(strategy);

    if (onToggle) {
      onToggle(strategy);
      return;
    }

    if (onSelect) {
      onSelect(strategy);
      return;
    }

    setLocalSelectedIds((previous) =>
      previous.includes(id)
        ? previous.filter((item) => item !== id)
        : [...previous, id]
    );
  }

  const headings: { key: SortKey; title: string }[] = [
    { key: "rank", title: "Rank" },
    { key: "bets", title: "Bets" },
    { key: "betsPerWeek", title: "Bets/wk" },
    { key: "profit", title: "Profit" },
    { key: "profitPerBet", title: "£/bet" },
    { key: "strikeRate", title: "Strike %" },
  ];

  return (
    <div className="min-w-0">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h3 className="font-serif text-xl text-[#123d2d]">
            Weekly model rankings
          </h3>

          <p className="mt-1 text-xs text-gray-500">
            Top 20 model strategies · at least 50 bets ·
            three-month discovery window
          </p>
        </div>

        {isAdmin && Boolean(data?.runs.length) && (
          <label className="text-sm">
            Model run
            <select
              value={data?.selectedRun ?? ""}
              onChange={(event) =>
                void load(event.target.value)
              }
              disabled={loading}
              className="ml-2 max-w-full rounded-lg border border-gray-300 bg-white px-3 py-2"
            >
              {data?.runs.map((run) => (
                <option
                  key={run.timestamp}
                  value={run.timestamp}
                >
                  {dateLabel(run.timestamp)} ({run.strategies})
                </option>
              ))}
            </select>
          </label>
        )}
      </div>

      {loading && (
        <p className="text-sm text-gray-500">
          Loading modelled scenarios…
        </p>
      )}

      {error && (
        <p role="alert" className="text-sm text-red-700">
          {error}
        </p>
      )}

      {!loading &&
        !error &&
        data?.strategies.length === 0 && (
          <p className="text-sm text-gray-500">
            No saved model runs found.
          </p>
        )}

      {!loading && data && data.strategies.length > 0 && (
        <>
          <div className="mb-3 flex flex-wrap items-center justify-between gap-3 text-sm">
            <p className="text-gray-500">
              Tick strategies to include them in the
              performance comparison below.
            </p>

            {isAdmin && (
              <label>
                Show
                <select
                  className="ml-2 rounded border border-gray-300 bg-white px-2 py-1"
                  value={limit}
                  onChange={(event) =>
                    setLimit(Number(event.target.value))
                  }
                >
                  <option value={5}>5</option>
                  <option value={10}>10</option>
                  <option value={20}>20</option>
                </select>
              </label>
            )}
          </div>

          <div className="w-full overflow-x-auto">
            <table className="w-full min-w-[680px] text-left text-xs md:text-sm">
              <thead className="bg-[#f7f4ed] text-[10px] uppercase text-gray-600 md:text-xs">
                <tr>
                  <th className="px-2 py-3">
                    Compare
                  </th>

                  {headings.slice(0, 1).map((heading) => (
                    <th key={heading.key} className="px-2 py-3">
                      <button
                        type="button"
                        onClick={() => changeSort(heading.key)}
                      >
                        {heading.title}
                        {sortKey === heading.key
                          ? sortDirection === "asc"
                            ? " ↑"
                            : " ↓"
                          : ""}
                      </button>
                    </th>
                  ))}

                  <th className="px-3 py-3">
                    Conditions
                  </th>

                  {headings.slice(1).map((heading) => (
                    <th
                      key={heading.key}
                      className="px-2 py-3 text-right"
                    >
                      <button
                        type="button"
                        onClick={() => changeSort(heading.key)}
                      >
                        {heading.title}
                        {sortKey === heading.key
                          ? sortDirection === "asc"
                            ? " ↑"
                            : " ↓"
                          : ""}
                      </button>
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody>
                {sorted
                  .slice(0, isAdmin ? limit : 5)
                  .map((strategy) => {
                    const id = modelStrategyId(strategy);
                    const selected = activeIds.includes(id);

                    return (
                      <tr
                        key={id}
                        className={`border-b border-gray-100 align-top ${
                          selected
                            ? "bg-[#edf4ed]"
                            : "hover:bg-gray-50"
                        }`}
                      >
                        <td className="px-2 py-3">
                          <input
                            type="checkbox"
                            checked={selected}
                            onChange={() =>
                              toggleStrategy(strategy)
                            }
                            aria-label={`Compare strategy ${strategy.rank}`}
                            className="accent-[#123d2d]"
                          />
                        </td>

                        <td className="px-2 py-3 font-semibold">
                          {strategy.rank}
                        </td>

                        <td className="px-3 py-3 text-[#123d2d]">
                          {formatModelConditions(
                            strategy.conditions
                          )}
                        </td>

                        <td className="px-2 py-3 text-right">
                          {strategy.bets}
                        </td>

                        <td className="px-2 py-3 text-right">
                          {strategy.betsPerWeek.toFixed(1)}
                        </td>

                        <td
                          className={`px-2 py-3 text-right ${
                            strategy.profit < 0
                              ? "text-red-700"
                              : "text-green-800"
                          }`}
                        >
                          {money(strategy.profit)}
                        </td>

                        <td className="px-2 py-3 text-right">
                          {money(strategy.profitPerBet)}
                        </td>

                        <td className="px-2 py-3 text-right">
                          {modelPercent(strategy.strikeRate)}
                        </td>
                      </tr>
                    );
                  })}
              </tbody>
            </table>
          </div>

          <p className="mt-3 text-xs text-gray-500">
            These figures represent the model discovery period,
            not the historical backtest. Backtests retain the
            original model rules, including exactly one
            qualifying horse per race.
          </p>
        </>
      )}
    </div>
  );
}
