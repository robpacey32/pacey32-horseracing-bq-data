"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { onAuthStateChanged, type User } from "firebase/auth";
import {
  collection,
  deleteDoc,
  doc,
  getDocs,
  type Timestamp,
} from "firebase/firestore";
import { auth, db } from "@/lib/firebase";

type SavedBet = {
  id: string;
  runnerId?: string;
  raceDate?: string;
  raceTime?: string;
  raceLocation?: string;
  raceName?: string;
  horseName?: string;
  horseNumber?: number | string | null;
  betType?: "WIN" | "EW";
  stake?: number;
  originalOdds?: string | null;
  takenOdds?: string;
  createdAt?: Timestamp | null;
};

type RaceResult = {
  runner_id: string;
  result: string | null;
  resultPosition: number | null;
  originalOdds: string | null;
  placeTerms: number | null;
  placesPaid: number | null;
  ewPossible: boolean | null;
};

type Settlement = {
  status: "Won" | "Placed" | "Lost" | "Pending" | "Unavailable";
  stake: number;
  returns: number | null;
  profit: number | null;
};

function fractionalOdds(value?: string): number | null {
  const odds = (value ?? "").trim().toUpperCase();
  if (odds === "EVS" || odds === "EVENS") return 1;
  const fraction = odds.match(/^(\d+(?:\.\d+)?)\s*\/\s*(\d+(?:\.\d+)?)$/);
  if (fraction) {
    const denominator = Number(fraction[2]);
    return denominator > 0 ? Number(fraction[1]) / denominator : null;
  }
  const decimal = Number(odds);
  return odds && Number.isFinite(decimal) && decimal > 1 ? decimal - 1 : null;
}

function settleBet(bet: SavedBet, result?: RaceResult): Settlement {
  const unitStake = Number(bet.stake) || 0;
  const isEW = bet.betType === "EW";
  const totalStake = unitStake * (isEW ? 2 : 1);
  if (!result) return { status: "Pending", stake: totalStake, returns: null, profit: null };

  const position = result.resultPosition == null ? null : Number(result.resultPosition);
  const hasResult = Boolean(result.result?.trim()) || position !== null;
  if (!hasResult) return { status: "Pending", stake: totalStake, returns: null, profit: null };

  const odds = fractionalOdds(bet.takenOdds);
  if (odds === null) return { status: "Unavailable", stake: totalStake, returns: null, profit: null };

  const won = position === 1;
  const placed = isEW && Boolean(result.ewPossible) && position !== null &&
    position >= 1 && position <= Number(result.placesPaid ?? 0);
  const winReturn = won ? unitStake * (1 + odds) : 0;
  const placeReturn = placed ? unitStake * (1 + odds * Number(result.placeTerms ?? 0)) : 0;
  const returns = winReturn + placeReturn;
  return {
    status: won ? "Won" : placed ? "Placed" : "Lost",
    stake: totalStake,
    returns,
    profit: returns - totalStake,
  };
}

const currency = (amount: number) =>
  new Intl.NumberFormat("en-GB", { style: "currency", currency: "GBP" }).format(amount);

export default function BetTrackerHistoryPage() {
  const [user, setUser] = useState<User | null>(null);
  const [checkingAuth, setCheckingAuth] = useState(true);
  const [bets, setBets] = useState<SavedBet[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [horseFilter, setHorseFilter] = useState("");
  const [results, setResults] = useState<Record<string, RaceResult>>({});
  const [resultsLoading, setResultsLoading] = useState(false);
  const [resultsError, setResultsError] = useState("");

  useEffect(() => {
    return onAuthStateChanged(auth, (currentUser) => {
      setUser(currentUser);
      setCheckingAuth(false);
    });
  }, []);

  useEffect(() => {
    if (!user) {
      setBets([]);
      return;
    }
    let cancelled = false;
    async function loadBets() {
      setLoading(true);
      setError("");
      try {
        const snapshot = await getDocs(collection(db, "users", user!.uid, "bets"));
        if (cancelled) return;
        const records = snapshot.docs.map((item) => ({
          ...item.data(),
          id: item.id,
        })) as SavedBet[];
        records.sort((a, b) =>
          (b.raceDate ?? "").localeCompare(a.raceDate ?? "") ||
          (b.raceTime ?? "").localeCompare(a.raceTime ?? "") ||
          (b.createdAt?.toMillis?.() ?? 0) - (a.createdAt?.toMillis?.() ?? 0)
        );
        setBets(records);
      } catch (err) {
        console.error("Failed to load bets:", err);
        if (!cancelled) setError("Unable to load your saved bets.");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    void loadBets();
    return () => { cancelled = true; };
  }, [user]);

  useEffect(() => {
    const runnerIds = [...new Set(bets.map((bet) => bet.runnerId).filter((id): id is string => Boolean(id)))];
    if (!user || !runnerIds.length) {
      setResults({});
      return;
    }
    let cancelled = false;
    async function loadResults() {
      setResultsLoading(true);
      setResultsError("");
      try {
        const batches: string[][] = [];
        for (let i = 0; i < runnerIds.length; i += 100) batches.push(runnerIds.slice(i, i + 100));
        const responses = await Promise.all(batches.map(async (batch) => {
          const response = await fetch("/api/bet-tracker/results", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ runnerIds: batch }),
          });
          if (!response.ok) throw new Error(`Results API returned ${response.status}`);
          const data = (await response.json()) as { results: RaceResult[] };
          return data.results;
        }));
        if (!cancelled) {
          const byId: Record<string, RaceResult> = {};
          for (const result of responses.flat()) byId[result.runner_id] = result;
          setResults(byId);
        }
      } catch (err) {
        console.error("Failed to retrieve race results:", err);
        if (!cancelled) setResultsError("Race results could not be loaded. Performance figures are unavailable.");
      } finally {
        if (!cancelled) setResultsLoading(false);
      }
    }
    void loadResults();
    return () => { cancelled = true; };
  }, [bets, user]);

  const visibleBets = useMemo(() => bets.filter((bet) =>
    (bet.horseName ?? "").toLowerCase().includes(horseFilter.trim().toLowerCase())
  ), [bets, horseFilter]);

  const settlements = useMemo(() => bets.map((bet) => ({
    bet,
    settlement: settleBet(bet, results[bet.runnerId ?? ""]),
  })), [bets, results]);

  const totalStaked = settlements.reduce((sum, row) => sum + row.settlement.stake, 0);
  const settled = resultsLoading || resultsError ? [] : settlements.filter((row) => row.settlement.profit !== null);
  const settledStake = settled.reduce((sum, row) => sum + row.settlement.stake, 0);
  const totalReturns = settled.reduce((sum, row) => sum + (row.settlement.returns ?? 0), 0);
  const profitLoss = totalReturns - settledStake;
  const roi = settledStake > 0 ? profitLoss / settledStake * 100 : null;
  const strikeRate = settled.length ? settled.filter((row) => row.settlement.status === "Won").length / settled.length * 100 : null;

  async function handleDelete(bet: SavedBet) {
    if (!user || deletingId) return;
    const confirmed = window.confirm(
      `Delete the ${bet.betType === "EW" ? "each-way" : "win"} bet on ${bet.horseName ?? "this horse"}? This cannot be undone.`
    );
    if (!confirmed) return;
    setDeletingId(bet.id);
    setError("");
    try {
      await deleteDoc(doc(db, "users", user.uid, "bets", bet.id));
      setBets((current) => current.filter((item) => item.id !== bet.id));
    } catch (err) {
      console.error("Failed to delete bet:", err);
      setError("Unable to delete that bet. Please try again.");
    } finally {
      setDeletingId(null);
    }
  }

  return (
    <div className="mx-auto max-w-7xl px-6 py-8">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-semibold text-gray-900">My Bets</h1>
          <p className="mt-2 text-gray-600">Review and manage your recorded bets.</p>
        </div>
        <Link href="/bet-tracker" className="rounded-lg border border-gray-300 px-4 py-2 font-medium text-gray-700 hover:bg-gray-50">
          Back to Bet Tracker
        </Link>
      </div>

      {checkingAuth ? (
        <p className="mt-8 text-gray-500">Checking authentication...</p>
      ) : !user ? (
        <p className="mt-8 rounded-lg border bg-white p-6 text-gray-700">
          Please <Link href="/bet-tracker" className="font-medium text-blue-700 underline">sign in to Bet Tracker</Link> to view your bets.
        </p>
      ) : (
        <>
          <div className="mt-8 grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            <div className="rounded-xl border border-gray-200 bg-white p-5">
              <p className="text-sm text-gray-500">Total bets</p>
              <p className="mt-1 text-3xl font-semibold text-gray-900">{bets.length}</p>
            </div>
            <div className="rounded-xl border border-gray-200 bg-white p-5">
              <p className="text-sm text-gray-500">Total committed stake</p>
              <p className="mt-1 text-3xl font-semibold text-gray-900">{currency(totalStaked)}</p>
            </div>
            {[
              ["Settled bets", String(settled.length)],
              ["Returns (settled)", resultsLoading || resultsError ? "—" : currency(totalReturns)],
              ["Profit / loss", resultsLoading || resultsError ? "—" : currency(profitLoss)],
              ["ROI (settled)", roi === null ? "—" : `${roi.toFixed(1)}%`],
              ["Win strike rate", strikeRate === null ? "—" : `${strikeRate.toFixed(1)}%`],
            ].map(([label, value]) => (
              <div key={label} className="rounded-xl border border-gray-200 bg-white p-5">
                <p className="text-sm text-gray-500">{label}</p>
                <p className="mt-1 text-3xl font-semibold text-gray-900">{value}</p>
              </div>
            ))}
          </div>
          {resultsLoading && <p className="mt-3 text-sm text-gray-500">Loading race results...</p>}
          {resultsError && <p role="alert" className="mt-3 text-sm text-red-700">{resultsError}</p>}

          <div className="mt-6 rounded-xl border border-gray-200 bg-white p-5">
            <label htmlFor="horse-filter" className="block text-sm font-medium text-gray-700">Search bets by horse</label>
            <input
              id="horse-filter"
              value={horseFilter}
              onChange={(event) => setHorseFilter(event.target.value)}
              placeholder="Horse name"
              className="mt-2 w-full max-w-md rounded-lg border border-gray-300 px-3 py-2"
            />
          </div>

          {error && <p role="alert" className="mt-4 rounded-lg bg-red-50 p-4 text-red-700">{error}</p>}
          {loading ? (
            <p className="mt-6 text-gray-500">Loading saved bets...</p>
          ) : visibleBets.length === 0 ? (
            <p className="mt-6 rounded-xl border border-gray-200 bg-white p-6 text-gray-600">
              {bets.length ? "No bets match that horse name." : "No bets recorded yet."}
            </p>
          ) : (
            <div className="mt-6 overflow-x-auto rounded-xl border border-gray-200 bg-white">
              <table className="min-w-full divide-y divide-gray-200 text-left text-sm">
                <thead className="bg-gray-50 text-gray-600">
                  <tr>
                    <th className="px-4 py-3">Date / time</th>
                    <th className="px-4 py-3">Horse</th>
                    <th className="px-4 py-3">Racecourse</th>
                    <th className="px-4 py-3">Bet</th>
                    <th className="px-4 py-3">Stake</th>
                    <th className="px-4 py-3">Odds taken</th>
                    <th className="px-4 py-3">Result</th>
                    <th className="px-4 py-3">Returns</th>
                    <th className="px-4 py-3">Profit / loss</th>
                    <th className="px-4 py-3">Action</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {visibleBets.map((bet) => (
                    <tr key={bet.id}>
                      <td className="whitespace-nowrap px-4 py-3">{bet.raceDate || "—"}<br /><span className="text-gray-500">{bet.raceTime || ""}</span></td>
                      <td className="px-4 py-3 font-medium text-gray-900">{bet.horseName || "—"}</td>
                      <td className="px-4 py-3">{bet.raceLocation || "—"}</td>
                      <td className="px-4 py-3">{bet.betType === "EW" ? "Each-way" : "Win"}</td>
                      <td className="whitespace-nowrap px-4 py-3">{currency(Number(bet.stake) || 0)}</td>
                      <td className="px-4 py-3">{bet.takenOdds || "—"}</td>
                      {(() => {
                        const settlement = resultsLoading || resultsError
                          ? null : settleBet(bet, results[bet.runnerId ?? ""]);
                        return (
                          <>
                            <td className="px-4 py-3">{settlement?.status ?? "—"}</td>
                            <td className="whitespace-nowrap px-4 py-3">{settlement?.returns == null ? "—" : currency(settlement.returns)}</td>
                            <td className="whitespace-nowrap px-4 py-3">{settlement?.profit == null ? "—" : currency(settlement.profit)}</td>
                          </>
                        );
                      })()}
                      <td className="px-4 py-3">
                        <button
                          type="button"
                          onClick={() => void handleDelete(bet)}
                          disabled={deletingId !== null}
                          className="font-medium text-red-700 hover:underline disabled:opacity-50"
                        >
                          {deletingId === bet.id ? "Deleting..." : "Delete"}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <p className="mt-4 text-sm text-gray-500">
            For each-way bets, the entered stake is applied to both the win and place portions.
            Performance figures exclude pending bets. Returns use the odds taken and the race's calculated place terms.
            Bets with missing results or invalid odds are not settled.
          </p>
        </>
      )}
    </div>
  );
}
