"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  GoogleAuthProvider,
  onAuthStateChanged,
  signInWithPopup,
  signOut,
  type User,
} from "firebase/auth";
import { collection, getDocs } from "firebase/firestore";
import { auth, db } from "@/lib/firebase";
import { settleBet, type SavedBetSummary, type RaceResultSummary } from "@/lib/betSettlement";
import BetTrackerRunners from "@/components/BetTrackerRunners";

type AccountStats = { bets: number; profit: number; roi: number | null };

const money = (value: number) =>
  new Intl.NumberFormat("en-GB", { style: "currency", currency: "GBP" }).format(value);

export default function BetTrackerPage() {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [stats, setStats] = useState<AccountStats | null>(null);
  const [statsLoading, setStatsLoading] = useState(false);
  const [statsError, setStatsError] = useState("");

  useEffect(() => {
    return onAuthStateChanged(auth, (currentUser) => {
      setUser(currentUser);
      setLoading(false);
    });
  }, []);

  useEffect(() => {
    if (!user) {
      setStats(null);
      return;
    }
    let cancelled = false;
    async function loadStats() {
      setStatsLoading(true);
      setStatsError("");
      setStats(null);
      try {
        const snapshot = await getDocs(collection(db, "users", user!.uid, "bets"));
        const bets = snapshot.docs.map((doc) => doc.data() as SavedBetSummary);
        const runnerIds = [...new Set(
          bets.map((bet) => bet.runnerId).filter((id): id is string => Boolean(id))
        )];
        const byId: Record<string, RaceResultSummary> = {};
        for (let i = 0; i < runnerIds.length; i += 100) {
          const response = await fetch("/api/bet-tracker/results", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ runnerIds: runnerIds.slice(i, i + 100) }),
          });
          if (!response.ok) throw new Error(`Results API returned ${response.status}`);
          const data = (await response.json()) as { results: RaceResultSummary[] };
          for (const result of data.results) byId[result.runner_id] = result;
        }
        const settled = bets
          .map((bet) => settleBet(bet, byId[bet.runnerId ?? ""]))
          .filter((row) => row.profit !== null);
        const stake = settled.reduce((sum, row) => sum + row.stake, 0);
        const profit = settled.reduce((sum, row) => sum + (row.profit ?? 0), 0);
        if (!cancelled) setStats({
          bets: bets.length,
          profit,
          roi: stake > 0 ? profit / stake * 100 : null,
        });
      } catch (err) {
        console.error("Failed to load account summary:", err);
        if (!cancelled) setStatsError("Performance unavailable");
      } finally {
        if (!cancelled) setStatsLoading(false);
      }
    }
    void loadStats();
    return () => { cancelled = true; };
  }, [user]);

  async function handleSignIn() {
    setError("");
    try {
      await signInWithPopup(auth, new GoogleAuthProvider());
    } catch (err) {
      console.error("Google sign-in failed:", err);
      setError("Unable to sign in with Google. Please try again.");
    }
  }

  async function handleSignOut() {
    setError("");
    try {
      await signOut(auth);
    } catch (err) {
      console.error("Sign-out failed:", err);
      setError("Unable to sign out. Please try again.");
    }
  }

  return (
    <div className="mx-auto max-w-7xl px-6 py-8">
      <h1 className="text-3xl font-semibold text-gray-900">Bet Tracker</h1>
      <p className="mt-2 text-gray-600">
        Record bets and analyse betting performance over time.
      </p>

      {loading ? (
        <p className="mt-8 text-gray-500">Checking authentication...</p>
      ) : !user ? (
        <div className="mt-8 rounded-lg border border-gray-200 bg-white p-6">
          <h2 className="text-xl font-semibold text-gray-900">Sign in</h2>
          <p className="mt-2 text-gray-600">
            Sign in with your Google account to access your private betting records.
          </p>
          <button
            onClick={handleSignIn}
            className="mt-6 rounded-xl border border-gray-300 bg-white px-6 py-3 text-sm font-semibold text-gray-700 shadow-sm hover:bg-gray-50"
          >
            Continue with Google
          </button>
        </div>
      ) : (
        <>
          <section className="mt-6 rounded-xl border border-gray-200 bg-white p-5">
            <div className="grid gap-4 md:grid-cols-[minmax(0,1.6fr)_repeat(3,minmax(0,1fr))]">
              <div className="flex min-w-0 flex-col justify-center gap-1">
                <p className="font-semibold text-gray-900">{user.displayName || "User"}</p>
                <p className="break-all text-sm text-gray-600">{user.email}</p>
                <button
                  type="button"
                  onClick={handleSignOut}
                  className="mt-2 w-fit text-sm font-medium text-gray-600 hover:text-gray-900 hover:underline"
                >
                  Sign out
                </button>
              </div>
              {[
                { label: "Bets", value: stats ? String(stats.bets) : "—" },
                { label: "P&L", value: stats ? money(stats.profit) : "—" },
                { label: "ROI", value: stats?.roi == null ? "—" : `${stats.roi.toFixed(1)}%` },
              ].map((metric) => (
                <div key={metric.label} className="rounded-lg bg-gray-50 px-4 py-4 text-center">
                  <p className="text-sm text-gray-500">{metric.label}</p>
                  <p className="mt-2 text-xl font-semibold tabular-nums text-gray-900">
                    {metric.value}
                  </p>
                </div>
              ))}
            </div>
            <div className="mt-3 flex flex-wrap items-center justify-between gap-2">
              <p className="text-xs text-gray-500">
                {statsLoading ? "Loading performance..." : statsError || "P&L and ROI include settled bets only."}
              </p>
              <Link
                href="/bet-tracker/history"
                className="inline-flex items-center justify-center rounded-lg border border-orange-500 bg-white px-5 py-2.5 font-medium text-orange-800 transition-colors hover:bg-orange-50"
              >
                More details →
              </Link>
            </div>
          </section>
          <div className="mt-6 border-t border-gray-200 pt-6">
            <BetTrackerRunners />
          </div>
        </>
      )}
      {error && <p className="mt-4 text-sm text-red-600">{error}</p>}
    </div>
  );
}
