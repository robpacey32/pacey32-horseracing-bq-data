"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

type ModelRun = { timestamp: string; strategies: number };
type ModelStrategy = {
  runTimestamp: string; lookbackMonths: number; rank: number; conditions: string;
  bets: number; betsPerWeek: number; activeDays: number; profit: number;
  profitPerBet: number; profitPerDay: number; modelRoi: number; strikeRate: number;
};
type ResponseData = { runs: ModelRun[]; selectedRun: string | null; strategies: ModelStrategy[]; error?: string };
type BacktestSummary = { bets: number; winners: number; strikeRate: number; staked: number; returns: number; profit: number; roi: number; maxDrawdown: number };
type DailyPerformance = { date: string; bets: number; winners: number; staked: number; returns: number; profit: number; cumulativeProfit: number; cumulativeROI: number };
type BacktestResult = { strategy: { runTimestamp: string; rank: number; conditions: string }; period: { startDate: string; endDate: string }; summary: BacktestSummary; dailyPerformance: DailyPerformance[] };
type SortKey = "rank" | "bets" | "betsPerWeek" | "profit" | "profitPerBet" | "strikeRate";
type BacktestMode = "historical" | "since_discovery";

const money = (n: number) => n.toLocaleString("en-GB", { style: "currency", currency: "GBP" });
const percent = (n: number) => `${n.toFixed(1)}%`;
const modelPercent = (n: number) => `${(n * 100).toFixed(1)}%`;
const dateLabel = (s: string) => new Date(s).toLocaleString("en-GB", { day: "2-digit", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" });
const shortDate = (s: string) => s.split("-").reverse().join("/");
const utcDate = (d: Date) => d.toISOString().slice(0, 10);
function addUtcDays(s: string, days: number) { const d = new Date(`${s}T00:00:00Z`); d.setUTCDate(d.getUTCDate() + days); return utcDate(d); }

function Metric({ label, value, negative }: { label: string; value: string; negative?: boolean }) {
  return <div className="rounded-lg border border-gray-200 bg-[#faf9f6] p-3"><p className="text-xs uppercase text-gray-500">{label}</p><p className={`mt-1 text-lg font-semibold ${negative ? "text-red-700" : "text-[#123d2d]"}`}>{value}</p></div>;
}

export default function ModelledScenarios() {
  const [data, setData] = useState<ResponseData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedRank, setSelectedRank] = useState<number | null>(null);
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [backtestLoading, setBacktestLoading] = useState(false);
  const [backtestError, setBacktestError] = useState<string | null>(null);
  const [backtest, setBacktest] = useState<BacktestResult | null>(null);
  const [backtestMode, setBacktestMode] = useState<BacktestMode>("historical");
  const [limit, setLimit] = useState(5);
  const [sortKey, setSortKey] = useState<SortKey>("rank");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");

  useEffect(() => {
    const now = new Date(); const start = new Date(now); start.setUTCMonth(start.getUTCMonth() - 3);
    setStartDate(utcDate(start)); setEndDate(utcDate(now));
  }, []);

  const load = useCallback(async (run?: string) => {
    setLoading(true); setError(null); setSelectedRank(null); setBacktest(null); setBacktestError(null);
    try {
      const response = await fetch(`/api/scenarios/modelled${run ? `?run=${encodeURIComponent(run)}` : ""}`, { cache: "no-store" });
      const json = (await response.json()) as ResponseData;
      if (!response.ok) throw new Error(json.error || "Unable to load modelled scenarios.");
      setData(json);
    } catch (err) { setError(err instanceof Error ? err.message : "Unable to load modelled scenarios."); }
    finally { setLoading(false); }
  }, []);
  useEffect(() => { void load(); }, [load]);

  const sorted = useMemo(() => [...(data?.strategies ?? [])].sort((a, b) => {
    const diff = a[sortKey] - b[sortKey];
    return sortDirection === "asc" ? diff : -diff;
  }), [data, sortKey, sortDirection]);
  const selected = data?.strategies.find(s => s.rank === selectedRank) ?? null;
  function changeSort(key: SortKey) {
    if (key === sortKey) setSortDirection(prev => prev === "asc" ? "desc" : "asc");
    else { setSortKey(key); setSortDirection(key === "rank" ? "asc" : "desc"); }
  }
  function select(rank: number) { setSelectedRank(rank); setBacktest(null); setBacktestError(null); }

  async function runBacktest(mode: BacktestMode) {
    if (!selected || !data?.selectedRun) return;
    const effectiveStart = mode === "since_discovery" ? addUtcDays(data.selectedRun.slice(0, 10), 1) : startDate;
    const effectiveEnd = mode === "since_discovery" ? utcDate(new Date()) : endDate;
    if (!effectiveStart || !effectiveEnd || effectiveStart > effectiveEnd) {
      setBacktestError("Choose a valid date range. Since Discovery requires at least one completed day."); return;
    }
    setBacktestLoading(true); setBacktestError(null); setBacktest(null); setBacktestMode(mode);
    try {
      const response = await fetch("/api/scenarios/modelled/backtest", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ runTimestamp: data.selectedRun, rank: selected.rank, startDate: effectiveStart, endDate: effectiveEnd }),
      });
      const json = await response.json();
      if (!response.ok) throw new Error(json.error || "Unable to run backtest.");
      setBacktest(json as BacktestResult);
    } catch (err) { setBacktestError(err instanceof Error ? err.message : "Unable to run backtest."); }
    finally { setBacktestLoading(false); }
  }

  const headings: { key: SortKey; title: string }[] = [
    { key: "rank", title: "Rank" }, { key: "bets", title: "Bets" }, { key: "betsPerWeek", title: "Bets/wk" },
    { key: "profit", title: "Profit" }, { key: "profitPerBet", title: "£/bet" }, { key: "strikeRate", title: "Strike %" },
  ];

  return <div className="min-w-0">
    <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
      <div><h3 className="font-serif text-xl text-[#123d2d]">Weekly model rankings</h3><p className="text-xs text-gray-500">Top 20 · at least 50 bets · three-month discovery window</p></div>
      {data?.runs.length ? <label className="text-sm">Model run <select value={data.selectedRun ?? ""} onChange={e => void load(e.target.value)} disabled={loading} className="ml-2 max-w-full rounded-lg border border-gray-300 bg-white px-3 py-2">{data.runs.map(r => <option key={r.timestamp} value={r.timestamp}>{dateLabel(r.timestamp)} ({r.strategies})</option>)}</select></label> : null}
    </div>
    {loading && <p className="text-sm text-gray-500">Loading modelled scenarios…</p>}
    {error && <p className="text-sm text-red-700">{error}</p>}
    {!loading && !error && data?.strategies.length === 0 && <p className="text-sm text-gray-500">No saved model runs found.</p>}
    {data && data.strategies.length > 0 && <>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3 text-sm">
        <p className="text-gray-500">Click a row to analyse it. Click column headings to sort.</p>
        <label>Show <select className="ml-1 rounded border border-gray-300 bg-white px-2 py-1" value={limit} onChange={e => setLimit(Number(e.target.value))}><option value={5}>5</option><option value={10}>10</option><option value={20}>20</option></select></label>
      </div>
      <div className="w-full overflow-x-auto">
        <table className="w-full table-fixed text-left text-xs md:text-sm">
          <colgroup><col className="w-[6%]" /><col className="w-[44%]" /><col className="w-[8%]" /><col className="w-[9%]" /><col className="w-[11%]" /><col className="w-[10%]" /><col className="w-[12%]" /></colgroup>
          <thead className="bg-[#f7f4ed] text-[10px] uppercase text-gray-600 md:text-xs"><tr>
            <th className="px-1 py-3 md:px-2"><button type="button" onClick={() => changeSort("rank")}>Rank {sortKey === "rank" ? (sortDirection === "asc" ? "↑" : "↓") : ""}</button></th>
            <th className="px-2 py-3">Conditions</th>
            {headings.slice(1).map(h => <th key={h.key} className="px-1 py-3 text-right md:px-2"><button type="button" onClick={() => changeSort(h.key)}>{h.title} {sortKey === h.key ? (sortDirection === "asc" ? "↑" : "↓") : ""}</button></th>)}
          </tr></thead>
          <tbody>{sorted.slice(0, limit).map(s => <tr key={`${s.runTimestamp}-${s.rank}`} onClick={() => select(s.rank)} onKeyDown={e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); select(s.rank); } }} tabIndex={0} aria-selected={s.rank === selectedRank} className={`cursor-pointer border-b border-gray-100 align-top focus:outline focus:outline-2 focus:outline-[#123d2d] ${s.rank === selectedRank ? "bg-[#edf4ed]" : "hover:bg-gray-50"}`}>
            <td className="px-1 py-3 font-semibold md:px-2">{s.rank}</td>
            <td className="break-words px-2 py-3 text-[#123d2d]">{s.conditions}</td>
            <td className="px-1 py-3 text-right md:px-2">{s.bets}</td>
            <td className="px-1 py-3 text-right md:px-2">{s.betsPerWeek.toFixed(1)}</td>
            <td className={`px-1 py-3 text-right md:px-2 ${s.profit < 0 ? "text-red-700" : "text-green-800"}`}>{money(s.profit)}</td>
            <td className="px-1 py-3 text-right md:px-2">{money(s.profitPerBet)}</td>
            <td className="px-1 py-3 text-right md:px-2">{modelPercent(s.strikeRate)}</td>
          </tr>)}</tbody>
        </table>
      </div>
      <p className="mt-2 text-xs text-gray-500">Model statistics are from the discovery period. Click any strategy to backtest it; rankings are unchanged when sorting.</p>
    </>}

    {selected && <div className="mt-6 border-t border-gray-200 pt-5">
      <h3 className="font-serif text-xl text-[#123d2d]">Strategy #{selected.rank}</h3>
      <p className="mt-1 break-words text-sm text-gray-600">{selected.conditions}</p>
      <div className="mt-4 grid grid-cols-2 gap-2 md:grid-cols-4">
        <Metric label="Discovery bets" value={String(selected.bets)} />
        <Metric label="Discovery profit" value={money(selected.profit)} negative={selected.profit < 0} />
        <Metric label="Profit / bet" value={money(selected.profitPerBet)} negative={selected.profitPerBet < 0} />
        <Metric label="Strike rate" value={modelPercent(selected.strikeRate)} />
      </div>
      <div className="mt-4 rounded-lg border border-[#ded8c9] bg-[#faf9f6] p-4">
        <h4 className="mb-3 font-semibold text-[#123d2d]">Test strategy performance</h4>
        <div className="flex flex-wrap items-end gap-3">
          <label className="text-sm">Start date<input type="date" value={startDate} onChange={e => setStartDate(e.target.value)} className="mt-1 block rounded-lg border border-gray-300 bg-white px-2 py-2" /></label>
          <label className="text-sm">End date<input type="date" value={endDate} onChange={e => setEndDate(e.target.value)} className="mt-1 block rounded-lg border border-gray-300 bg-white px-2 py-2" /></label>
          <button type="button" disabled={backtestLoading} onClick={() => void runBacktest("historical")} className="rounded-lg bg-[#123d2d] px-4 py-2.5 text-sm font-semibold text-white disabled:opacity-50">Run historical backtest</button>
          <button type="button" disabled={backtestLoading} onClick={() => void runBacktest("since_discovery")} className="rounded-lg border border-[#123d2d] bg-white px-4 py-2.5 text-sm font-semibold text-[#123d2d] disabled:opacity-50">Since discovery</button>
        </div>
        <p className="mt-2 text-xs text-gray-500">Since discovery starts the day after the model run. The current source view covers approximately 12 months.</p>
      </div>
      {backtestLoading && <p className="mt-4 text-sm text-gray-500">Calculating…</p>}
      {backtestError && <p className="mt-4 text-sm text-red-700">{backtestError}</p>}
      {backtest && <div className="mt-5">
        <h4 className="font-serif text-xl text-[#123d2d]">{backtestMode === "since_discovery" ? "Performance since discovery" : "Historical backtest"}</h4>
        <p className="mb-3 text-xs text-gray-500">{shortDate(backtest.period.startDate)} – {shortDate(backtest.period.endDate)}</p>
        <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
          <Metric label="Bets" value={String(backtest.summary.bets)} />
          <Metric label="Winners" value={String(backtest.summary.winners)} />
          <Metric label="Strike rate" value={percent(backtest.summary.strikeRate)} />
          <Metric label="ROI" value={percent(backtest.summary.roi)} negative={backtest.summary.roi < 0} />
          <Metric label="Staked" value={money(backtest.summary.staked)} />
          <Metric label="Returns" value={money(backtest.summary.returns)} />
          <Metric label="Profit" value={money(backtest.summary.profit)} negative={backtest.summary.profit < 0} />
          <Metric label="Max drawdown" value={money(backtest.summary.maxDrawdown)} />
        </div>
        {backtest.dailyPerformance.length > 0 && <div className="mt-4 rounded-lg border border-gray-200 p-4"><h5 className="mb-3 font-semibold text-[#123d2d]">Cumulative profit</h5><div className="h-[280px] w-full"><ResponsiveContainer width="100%" height="100%"><LineChart data={backtest.dailyPerformance} margin={{ top: 8, right: 16, left: 4, bottom: 8 }}><CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" /><XAxis dataKey="date" tickFormatter={shortDate} tick={{ fontSize: 10 }} /><YAxis tickFormatter={v => `£${v}`} tick={{ fontSize: 10 }} /><Tooltip formatter={v => [money(Number(v ?? 0)), "Cumulative profit"]} labelFormatter={v => shortDate(String(v))} /><ReferenceLine y={0} stroke="#6b7280" strokeDasharray="4 4" /><Line type="monotone" dataKey="cumulativeProfit" stroke="#b49a45" strokeWidth={2.5} dot={false} /></LineChart></ResponsiveContainer></div></div>}
        {backtest.summary.bets === 0 && <p className="mt-3 text-sm text-gray-500">No qualifying selections found in this period.</p>}
        <p className="mt-2 text-xs text-gray-500">Backtests use the original model's rules and exactly one qualifying horse per race.</p>
      </div>}
    </div>}
  </div>;
}
