
"use client";

import { useEffect, useState, type FormEvent } from "react";
import Link from "next/link";
import {
  GoogleAuthProvider,
  onAuthStateChanged,
  signInWithPopup,
  signInWithEmailAndPassword,
  createUserWithEmailAndPassword,
  sendEmailVerification,
  sendPasswordResetEmail,
  signOut,
  updateProfile,
  reload,
  type User,
} from "firebase/auth";
import { auth } from "@/lib/firebase";

type AuthMode = "signin" | "register" | "reset";

function getErrorMessage(error: unknown): string {
  const code =
    typeof error === "object" && error !== null && "code" in error
      ? String(error.code)
      : "";

  switch (code) {
    case "auth/email-already-in-use":
      return "This email is already registered. Please sign in.";
    case "auth/invalid-email":
      return "Please enter a valid email address.";
    case "auth/weak-password":
      return "Please choose a stronger password.";
    case "auth/invalid-credential":
    case "auth/wrong-password":
    case "auth/user-not-found":
      return "Incorrect email address or password.";
    case "auth/too-many-requests":
      return "Too many attempts. Please try again later.";
    case "auth/popup-closed-by-user":
      return "Google sign-in was cancelled.";
    case "auth/account-exists-with-different-credential":
      return "An account exists with a different sign-in method.";
    default:
      return "Something went wrong. Please try again.";
  }
}

export default function AccountPage() {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [mode, setMode] = useState<AuthMode>("signin");

  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);

  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  useEffect(() => {
    return onAuthStateChanged(auth, (currentUser) => {
      setUser(currentUser);
      setLoading(false);
    });
  }, []);

  function changeMode(nextMode: AuthMode) {
    setMode(nextMode);
    setError("");
    setMessage("");
    setPassword("");
    setConfirmPassword("");
    setShowPassword(false);
  }

  async function handleGoogleSignIn() {
    setError("");
    setMessage("");
    setBusy(true);

    try {
      await signInWithPopup(auth, new GoogleAuthProvider());
    } catch (err) {
      console.error(err);
      setError(getErrorMessage(err));
    } finally {
      setBusy(false);
    }
  }

  async function handleEmailSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    setMessage("");

    const cleanEmail = email.trim().toLowerCase();

    if (mode === "register") {
      if (!firstName.trim() || !lastName.trim()) {
        setError("Please enter your first and last name.");
        return;
      }

      if (password.length < 8) {
        setError("Password must be at least 8 characters.");
        return;
      }

      if (password !== confirmPassword) {
        setError("Passwords do not match.");
        return;
      }
    }

    setBusy(true);

    try {
      if (mode === "reset") {
        await sendPasswordResetEmail(auth, cleanEmail);
        setMessage(
          "If an account exists for this email, a password reset link will be sent."
        );
      } else if (mode === "register") {
        const credential = await createUserWithEmailAndPassword(
          auth,
          cleanEmail,
          password
        );

        const displayName =
          `${firstName.trim()} ${lastName.trim()}`;

        try {
          await updateProfile(credential.user, { displayName });
          await sendEmailVerification(credential.user);

          setMessage(
            "Account created. Please check your email for the verification link."
          );
        } catch (setupError) {
          console.error("Account setup error:", setupError);
          setError(
            "Account created, but profile setup or verification email failed. Please retry verification below."
          );
        }

        setUser(auth.currentUser);
        setPassword("");
        setConfirmPassword("");
      } else {
        await signInWithEmailAndPassword(auth, cleanEmail, password);
        setPassword("");
      }
    } catch (err) {
      setError(getErrorMessage(err));
    } finally {
      setBusy(false);
    }
  }

  async function handleResendVerification() {
    if (!user) return;

    setError("");
    setMessage("");
    setBusy(true);

    try {
      await sendEmailVerification(user);
      setMessage("Verification email sent. Please check your inbox.");
    } catch (err) {
      console.error(err);
      setError(getErrorMessage(err));
    } finally {
      setBusy(false);
    }
  }

  async function handleCheckVerification() {
    if (!user) return;

    setError("");
    setMessage("");
    setBusy(true);

    try {
      await reload(user);
      setUser(auth.currentUser);

      if (auth.currentUser?.emailVerified) {
        setMessage("Your email address has been verified.");
      } else {
        setMessage("Email not verified yet. Please check your inbox.");
      }
    } catch (err) {
      console.error(err);
      setError(getErrorMessage(err));
    } finally {
      setBusy(false);
    }
  }

  async function handleSignOut() {
    setError("");
    setMessage("");
    setBusy(true);

    try {
      await signOut(auth);
      changeMode("signin");
    } catch (err) {
      console.error(err);
      setError("Unable to sign out.");
    } finally {
      setBusy(false);
    }
  }

  const needsVerification =
    user !== null &&
    user.providerData.some(
      (provider) => provider.providerId === "password"
    ) &&
    !user.emailVerified;

  const inputClass =
    "mt-1 w-full rounded-lg border border-gray-300 bg-white px-3 py-3 text-gray-900 outline-none focus:border-green-800 focus:ring-1 focus:ring-green-800";

  return (
    <main className="mx-auto max-w-5xl px-6 py-8">
      <h1 className="text-3xl font-semibold text-gray-900">
        My Account
      </h1>

      <p className="mt-2 text-gray-600">
        Manage your Pacey32 Racing account, scenarios and betting history.
      </p>

      {loading ? (
        <p className="mt-8 text-gray-600">
          Checking authentication...
        </p>
      ) : !user ? (
        <section className="mt-8 max-w-lg overflow-hidden rounded-xl border border-gray-300 bg-white">
          {/* Authentication tabs */}
          <div className="grid grid-cols-2 border-b border-gray-200">
            <button
              type="button"
              onClick={() => changeMode("signin")}
              className={`border-b-4 px-5 py-4 text-center font-semibold transition-colors ${
                mode === "signin" || mode === "reset"
                  ? "border-green-900 bg-green-50 text-green-900"
                  : "border-transparent bg-gray-50 text-gray-500 hover:bg-gray-100"
              }`}
            >
              Sign in
            </button>

            <button
              type="button"
              onClick={() => changeMode("register")}
              className={`border-b-4 px-5 py-4 text-center font-semibold transition-colors ${
                mode === "register"
                  ? "border-green-900 bg-green-50 text-green-900"
                  : "border-transparent bg-gray-50 text-gray-500 hover:bg-gray-100"
              }`}
            >
              Register
            </button>
          </div>

          <div className="p-6">
            <h2 className="text-xl font-semibold text-gray-900">
              {mode === "signin"
                ? "Welcome back"
                : mode === "register"
                  ? "Create your account"
                  : "Reset your password"}
            </h2>

            <p className="mt-2 text-sm text-gray-600">
              {mode === "signin"
                ? "Sign in to access your racing account."
                : mode === "register"
                  ? "Register to save scenarios, track bets and manage racing alerts."
                  : "Enter your email address to receive a password reset link."}
            </p>

            <form
              onSubmit={handleEmailSubmit}
              className="mt-6 space-y-4"
            >
              {mode === "register" && (
                <div className="grid gap-4 sm:grid-cols-2">
                  <div>
                    <label
                      htmlFor="first-name"
                      className="block text-sm font-medium text-gray-700"
                    >
                      First name
                    </label>
                    <input
                      id="first-name"
                      type="text"
                      autoComplete="given-name"
                      required
                      value={firstName}
                      onChange={(e) => setFirstName(e.target.value)}
                      className={inputClass}
                    />
                  </div>

                  <div>
                    <label
                      htmlFor="last-name"
                      className="block text-sm font-medium text-gray-700"
                    >
                      Last name
                    </label>
                    <input
                      id="last-name"
                      type="text"
                      autoComplete="family-name"
                      required
                      value={lastName}
                      onChange={(e) => setLastName(e.target.value)}
                      className={inputClass}
                    />
                  </div>
                </div>
              )}

              <div>
                <label
                  htmlFor="account-email"
                  className="block text-sm font-medium text-gray-700"
                >
                  Email address
                </label>
                <input
                  id="account-email"
                  type="email"
                  autoComplete="email"
                  required
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className={inputClass}
                />
              </div>

              {mode !== "reset" && (
                <div>
                  <label
                    htmlFor="account-password"
                    className="block text-sm font-medium text-gray-700"
                  >
                    Password
                  </label>

                  <div className="relative mt-1">
                    <input
                      id="account-password"
                      type={showPassword ? "text" : "password"}
                      autoComplete={
                        mode === "register"
                          ? "new-password"
                          : "current-password"
                      }
                      required
                      minLength={mode === "register" ? 8 : undefined}
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      className="w-full rounded-lg border border-gray-300 bg-white px-3 py-3 pr-20 text-gray-900 outline-none focus:border-green-800 focus:ring-1 focus:ring-green-800"
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword(!showPassword)}
                      className="absolute inset-y-0 right-3 text-sm font-medium text-green-900"
                    >
                      {showPassword ? "Hide" : "Show"}
                    </button>
                  </div>

                  {mode === "register" && (
                    <p className="mt-1 text-xs text-gray-500">
                      Minimum 8 characters.
                    </p>
                  )}
                </div>
              )}

              {mode === "register" && (
                <div>
                  <label
                    htmlFor="confirm-password"
                    className="block text-sm font-medium text-gray-700"
                  >
                    Confirm password
                  </label>
                  <input
                    id="confirm-password"
                    type={showPassword ? "text" : "password"}
                    autoComplete="new-password"
                    required
                    minLength={8}
                    value={confirmPassword}
                    onChange={(e) =>
                      setConfirmPassword(e.target.value)
                    }
                    className={inputClass}
                  />

                  {confirmPassword &&
                    password !== confirmPassword && (
                      <p className="mt-1 text-xs text-red-700">
                        Passwords do not match.
                      </p>
                    )}
                </div>
              )}

              <button
                type="submit"
                disabled={busy}
                className="w-full rounded-lg bg-green-900 px-5 py-3 font-medium text-white transition-colors hover:bg-green-800 disabled:opacity-50"
              >
                {busy
                  ? "Please wait..."
                  : mode === "signin"
                    ? "Sign in"
                    : mode === "register"
                      ? "Create account"
                      : "Send reset email"}
              </button>
            </form>

            {mode === "signin" && (
              <>
                <div className="my-5 flex items-center gap-3">
                  <div className="h-px flex-1 bg-gray-200" />
                  <span className="text-sm text-gray-500">or</span>
                  <div className="h-px flex-1 bg-gray-200" />
                </div>

                <button
                  type="button"
                  onClick={handleGoogleSignIn}
                  disabled={busy}
                  className="w-full rounded-lg border border-gray-300 bg-white px-5 py-3 font-medium text-gray-800 hover:bg-gray-50 disabled:opacity-50"
                >
                  Continue with Google
                </button>

                <button
                  type="button"
                  onClick={() => changeMode("reset")}
                  className="mt-5 text-sm font-medium text-green-900 underline"
                >
                  Forgot password?
                </button>
              </>
            )}

            {mode === "reset" && (
              <button
                type="button"
                onClick={() => changeMode("signin")}
                className="mt-5 text-sm font-medium text-green-900 underline"
              >
                Back to sign in
              </button>
            )}

            {error && (
              <p
                role="alert"
                className="mt-4 rounded-lg bg-red-50 p-3 text-sm text-red-700"
              >
                {error}
              </p>
            )}

            {message && (
              <p
                role="status"
                className="mt-4 rounded-lg bg-green-50 p-3 text-sm text-green-800"
              >
                {message}
              </p>
            )}
          </div>
        </section>
      ) : (
        <>
          <section className="mt-8 rounded-xl border bg-white p-6">
            <h2 className="text-xl font-semibold">Profile</h2>

            <p className="mt-4 font-medium">
              {user.displayName || "User"}
            </p>
            <p className="text-sm text-gray-600">{user.email}</p>

            {needsVerification ? (
              <div className="mt-5 rounded-lg border border-amber-300 bg-amber-50 p-4">
                <p className="font-medium text-amber-900">
                  Email verification required
                </p>
                <p className="mt-1 text-sm text-amber-800">
                  Please check your inbox and verify your email address.
                </p>

                <div className="mt-3 flex flex-wrap gap-4">
                  <button
                    onClick={handleResendVerification}
                    disabled={busy}
                    className="text-sm font-medium text-green-900 underline disabled:opacity-50"
                  >
                    Resend verification email
                  </button>

                  <button
                    onClick={handleCheckVerification}
                    disabled={busy}
                    className="text-sm font-medium text-green-900 underline disabled:opacity-50"
                  >
                    I&apos;ve verified my email
                  </button>
                </div>
              </div>
            ) : (
              <p className="mt-3 text-sm text-green-800">
                Email verified
              </p>
            )}

            <button
              onClick={handleSignOut}
              disabled={busy}
              className="mt-5 text-sm font-medium text-gray-700 underline disabled:opacity-50"
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

          {error && (
            <p role="alert" className="mt-4 text-sm text-red-700">
              {error}
            </p>
          )}

          {message && (
            <p role="status" className="mt-4 text-sm text-green-800">
              {message}
            </p>
          )}
        </>
      )}
    </main>
  );
}
