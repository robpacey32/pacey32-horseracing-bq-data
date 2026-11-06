
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
import { auth } from "@/lib/firebase";

export default function AccountPage() {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    return onAuthStateChanged(auth, (currentUser) => {
      setUser(currentUser);
      setLoading(false);
    });
  }, []);

  async function handleSignIn() {
    setError("");
    try {
      await signInWithPopup(auth, new GoogleAuthProvider());
    } catch (err) {
      console.error(err);
      setError("Unable to sign in with Google.");
    }
  }

  async function handleSignOut() {
    setError("");
    try {
      await signOut(auth);
    } catch (err) {
      console.error(err);
      setError("Unable to sign out.");
    }
  }

  return (
    <main className="mx-auto max-w-5xl px-6 py-8">
      <h1 className="text-3xl font-semibold text-gray-900">
        My Account
      </h1>
      <p className="mt-2 text-gray-600">
        Manage your Pacey32 Racing account, scenarios and betting history.
      </p>

      {loading ? (
        <p className="mt-8 text-gray-600">Checking authentication...</p>
      ) : !user ? (
        <section className="mt-8 rounded-xl border bg-white p-6">
          <h2 className="text-xl font-semibold">Sign in</h2>
          <p className="mt-2 text-gray-600">
            Sign in to access your saved scenarios and betting records.
          </p>
          <button
            onClick={handleSignIn}
            className="mt-5 rounded-lg bg-green-900 px-5 py-3 text-white"
          >
            Continue with Google
          </button>
        </section>
      ) : (
        <>
          <section className="mt-8 rounded-xl border bg-white p-6">
            <h2 className="text-xl font-semibold">Profile</h2>
            <p className="mt-4 font-medium">{user.displayName || "User"}</p>
            <p className="text-sm text-gray-600">{user.email}</p>
            <button
              onClick={handleSignOut}
              className="mt-4 text-sm font-medium text-gray-700 underline"
            >
              Sign out
            </button>
          </section>

          <div className="mt-6 grid gap-5 md:grid-cols-2">
            <section className="rounded-xl border bg-white p-6">
              <h2 className="text-xl font-semibold">My Scenarios</h2>
              <p className="mt-2 text-sm text-gray-600">
                Save and manage your racing scenarios.
              </p>
              <Link
                href="/scenarios"
                className="mt-4 inline-block text-green-900 underline"
              >
                Open Scenario Explorer
              </Link>
            </section>

            <section className="rounded-xl border bg-white p-6">
              <h2 className="text-xl font-semibold">My Betting</h2>
              <p className="mt-2 text-sm text-gray-600">
                Review your betting history and performance.
              </p>
              <Link
                href="/bet-tracker/history"
                className="mt-4 inline-block text-green-900 underline"
              >
                View Bet History
              </Link>
            </section>
          </div>
        </>
      )}

      {error && (
        <p role="alert" className="mt-4 text-sm text-red-700">
          {error}
        </p>
      )}
    </main>
  );
}
