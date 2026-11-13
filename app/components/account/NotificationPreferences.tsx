
"use client";

import { useEffect, useState } from "react";
import type { User } from "firebase/auth";

import type { SavedScenario } from "@/lib/scenarios/storage";
import {
  getCloudScenarios,
  updateScenarioNotifications,
} from "@/lib/scenarios/cloudStorage";
import { formatScenarioFilters } from "@/components/scenarios/SavedScenarios";
import {
  getDailySelectionsPreference,
  setDailySelectionsPreference,
} from "@/lib/notifications/preferences";

interface Props {
  user: User;
}

const MAX_ENABLED = 10;
const TELEGRAM_BOT = "Pacey32HorseAlerts_bot";

type ConnectionStatus =
  | "loading"
  | "connected"
  | "disconnected";

export default function NotificationPreferences({
  user,
}: Props) {
  const [scenarios, setScenarios] = useState<SavedScenario[]>([]);
  const [loading, setLoading] = useState(true);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [error, setError] = useState("");

  const [dailyEnabled, setDailyEnabled] = useState(true);
  const [dailyLoading, setDailyLoading] = useState(true);
  const [dailyBusy, setDailyBusy] = useState(false);

  const [connectionStatus, setConnectionStatus] =
    useState<ConnectionStatus>("loading");
  const [connectionBusy, setConnectionBusy] = useState(false);
  const [telegramLink, setTelegramLink] = useState("");
  const [connectionMessage, setConnectionMessage] = useState("");

  // --------------------------------------------------
  // Load saved scenarios
  // --------------------------------------------------

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      setError("");

      try {
        const saved = await getCloudScenarios(user.uid);

        if (!cancelled) {
          setScenarios(saved);
        }
      } catch {
        if (!cancelled) {
          setError("Unable to load saved scenarios.");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void load();

    return () => {
      cancelled = true;
    };
  }, [user.uid]);

  // --------------------------------------------------
  // Load Pacey32 daily selections preference
  // --------------------------------------------------

  useEffect(() => {
    let cancelled = false;

    setDailyLoading(true);

    getDailySelectionsPreference(user.uid)
      .then((enabled) => {
        if (!cancelled) {
          setDailyEnabled(enabled);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setError(
            "Unable to load daily selection preferences."
          );
        }
      })
      .finally(() => {
        if (!cancelled) {
          setDailyLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [user.uid]);

  // --------------------------------------------------
  // Load Telegram connection status
  // --------------------------------------------------

  useEffect(() => {
    let cancelled = false;

    async function loadConnection() {
      setConnectionStatus("loading");
      setConnectionMessage("");
      setTelegramLink("");

      try {
        const token = await user.getIdToken();

        const response = await fetch(
          "/api/telegram/status",
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${token}`,
            },
            cache: "no-store",
          }
        );

        if (!response.ok) {
          throw new Error(
            "Unable to check Telegram connection."
          );
        }

        const data: { connected: boolean } =
          await response.json();

        if (!cancelled) {
          setConnectionStatus(
            data.connected ? "connected" : "disconnected"
          );
        }
      } catch {
        if (!cancelled) {
          setConnectionStatus("disconnected");
          setConnectionMessage(
            "Could not verify your Telegram connection. Try Check Connection."
          );
        }
      }
    }

    void loadConnection();

    return () => {
      cancelled = true;
    };
  }, [user]);

  const enabledCount = scenarios.filter(
    (scenario) => scenario.notificationsEnabled === true
  ).length;

  // --------------------------------------------------
  // Toggle Pacey32 daily selections
  // --------------------------------------------------

  async function handleDailyToggle() {
    if (dailyBusy || dailyLoading) return;

    const nextEnabled = !dailyEnabled;

    setDailyBusy(true);
    setError("");

    try {
      await setDailySelectionsPreference(
        user.uid,
        nextEnabled
      );

      setDailyEnabled(nextEnabled);
    } catch {
      setError(
        "Unable to save daily selection preferences."
      );
    } finally {
      setDailyBusy(false);
    }
  }

  // --------------------------------------------------
  // Toggle saved scenario notifications
  // --------------------------------------------------

  async function handleToggle(scenario: SavedScenario) {
    if (busyId) return;

    const enabled = scenario.notificationsEnabled !== true;

    setBusyId(scenario.id);
    setError("");

    try {
      await updateScenarioNotifications(
        scenario.id,
        enabled
      );

      const updated = await getCloudScenarios(user.uid);
      setScenarios(updated);
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : "Unable to update notifications."
      );
    } finally {
      setBusyId(null);
    }
  }

  // --------------------------------------------------
  // Generate Telegram linking code
  // --------------------------------------------------

  async function handleConnectTelegram() {
    if (
      connectionBusy ||
      connectionStatus === "connected"
    ) {
      return;
    }

    setConnectionBusy(true);
    setConnectionMessage("");
    setTelegramLink("");

    try {
      const token = await user.getIdToken();

      const response = await fetch(
        "/api/telegram/link-code",
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
          },
          cache: "no-store",
        }
      );

      const data: {
        code?: string;
        expiresInSeconds?: number;
        error?: string;
      } = await response.json();

      if (!response.ok || !data.code) {
        throw new Error(
          data.error ||
            "Unable to generate a Telegram connection link."
        );
      }

      const link =
        `https://t.me/${TELEGRAM_BOT}?start=` +
        encodeURIComponent(data.code);

      setTelegramLink(link);

      setConnectionMessage(
        "Your connection link is ready. Open Telegram " +
          "and press Start. The link expires after 10 minutes."
      );
    } catch (err) {
      setConnectionMessage(
        err instanceof Error
          ? err.message
          : "Unable to connect Telegram."
      );
    } finally {
      setConnectionBusy(false);
    }
  }

  // --------------------------------------------------
  // Check Telegram connection
  // --------------------------------------------------

  async function handleCheckConnection() {
    if (connectionBusy) return;

    setConnectionBusy(true);
    setConnectionMessage("");

    try {
      const token = await user.getIdToken();

      const response = await fetch(
        "/api/telegram/status",
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
          },
          cache: "no-store",
        }
      );

      if (!response.ok) {
        throw new Error(
          "Unable to check Telegram connection."
        );
      }

      const data: { connected: boolean } =
        await response.json();

      if (data.connected) {
        setConnectionStatus("connected");
        setTelegramLink("");

        setConnectionMessage(
          "Your Telegram account is successfully connected."
        );
      } else {
        setConnectionStatus("disconnected");

        setConnectionMessage(
          "Telegram is not connected yet. " +
            "Open the connection link and press Start " +
            "in the bot, then check again."
        );
      }
    } catch (err) {
      setConnectionMessage(
        err instanceof Error
          ? err.message
          : "Unable to check Telegram connection."
      );
    } finally {
      setConnectionBusy(false);
    }
  }

  // --------------------------------------------------
  // Disconnect Telegram account
  // --------------------------------------------------

  async function handleDisconnectTelegram() {
    if (
      connectionBusy ||
      connectionStatus !== "connected"
    ) {
      return;
    }

    const confirmed = window.confirm(
      "Disconnect your Telegram account from Pacey32?\n\n" +
        "Your saved scenarios and notification preferences " +
        "will be preserved.\n\n" +
        "Your existing general Telegram broadcast " +
        "subscription will not be changed."
    );

    if (!confirmed) return;

    setConnectionBusy(true);
    setConnectionMessage("");

    try {
      const token = await user.getIdToken();

      const response = await fetch(
        "/api/telegram/disconnect",
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
          },
          cache: "no-store",
        }
      );

      const data: {
        disconnected?: boolean;
        syncPending?: boolean;
        message?: string;
        error?: string;
      } = await response.json();

      if (!response.ok || !data.disconnected) {
        throw new Error(
          data.error || "Unable to disconnect Telegram."
        );
      }

      // Firestore is the authoritative source.
      // The API confirms disconnection even if
      // the BigQuery mirror needs resynchronisation.

      setConnectionStatus("disconnected");
      setTelegramLink("");

      if (data.syncPending) {
        setConnectionMessage(
          "Your Telegram account has been disconnected " +
            "from Pacey32. However, the BigQuery mapping " +
            "still needs synchronisation."
        );
      } else {
        setConnectionMessage(
          "Your Telegram account has been disconnected. " +
            "You can reconnect it at any time."
        );
      }
    } catch (err) {
      setConnectionMessage(
        err instanceof Error
          ? err.message
          : "Unable to disconnect Telegram."
      );
    } finally {
      setConnectionBusy(false);
    }
  }

  // --------------------------------------------------
  // Render
  // --------------------------------------------------

  return (
    <section className="mt-6 rounded-xl border bg-white p-6">
      <h2 className="text-xl font-semibold">
        Notification Preferences
      </h2>

      <p className="mt-2 text-sm text-gray-600">
        Choose which selections you receive and how.
      </p>

      <p className="mt-3 text-sm font-medium">
        {enabledCount} / {MAX_ENABLED} scenarios enabled
      </p>

      {/* Notification preferences table */}

      <div className="mt-5 overflow-x-auto">
        <table className="w-full min-w-[650px] border-collapse text-left">
          <thead>
            <tr className="border-b border-gray-300 text-sm">
              <th className="py-3 pr-4 font-semibold">
                Selection
              </th>

              <th className="py-3 pr-4 font-semibold">
                Description
              </th>

              <th className="px-3 py-3 text-center font-semibold">
                Telegram
              </th>

              <th className="px-3 py-3 text-center font-semibold">
                iOS
              </th>
            </tr>
          </thead>

          <tbody className="divide-y divide-gray-200">
            {/* Pacey32's Daily Selections */}

            <tr>
              <td className="py-4 pr-4 text-sm font-semibold">
                Pacey32&apos;s Daily Selections
              </td>

              <td className="py-4 pr-4 text-sm text-gray-600">
                Morning selections and evening results
              </td>

              <td className="px-3 py-4 text-center">
                <button
                  type="button"
                  onClick={() => void handleDailyToggle()}
                  disabled={dailyBusy || dailyLoading}
                  aria-pressed={dailyEnabled}
                  aria-label="Toggle Pacey32 daily Telegram selections"
                  className={`rounded-lg border px-3 py-1.5 text-xs font-semibold disabled:opacity-40 ${
                    dailyEnabled
                      ? "border-green-800 bg-green-50 text-green-900"
                      : "border-gray-300 text-gray-600"
                  }`}
                >
                  {dailyBusy || dailyLoading
                    ? "Loading..."
                    : dailyEnabled
                      ? "On"
                      : "Off"}
                </button>
              </td>

              <td className="px-3 py-4 text-center text-xs text-gray-400">
                Coming soon
              </td>
            </tr>

            {/* User's saved scenarios */}

            {scenarios.map((scenario) => {
              const enabled =
                scenario.notificationsEnabled === true;

              return (
                <tr key={scenario.id}>
                  <td className="py-4 pr-4 text-sm font-medium">
                    {scenario.name}
                  </td>

                  <td className="py-4 pr-4 text-xs leading-relaxed text-gray-600">
                    {formatScenarioFilters(scenario.filters)}
                  </td>

                  <td className="px-3 py-4 text-center">
                    <button
                      type="button"
                      onClick={() =>
                        void handleToggle(scenario)
                      }
                      disabled={
                        busyId !== null ||
                        loading ||
                        (!enabled &&
                          enabledCount >= MAX_ENABLED)
                      }
                      aria-pressed={enabled}
                      aria-label={`Toggle Telegram notifications for ${scenario.name}`}
                      className={`rounded-lg border px-3 py-1.5 text-xs font-semibold disabled:opacity-40 ${
                        enabled
                          ? "border-green-800 bg-green-50 text-green-900"
                          : "border-gray-300 text-gray-600"
                      }`}
                    >
                      {busyId === scenario.id
                        ? "Saving..."
                        : enabled
                          ? "On"
                          : "Off"}
                    </button>
                  </td>

                  <td className="px-3 py-4 text-center text-xs text-gray-400">
                    Coming soon
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {loading && (
        <p className="mt-3 text-sm text-gray-500">
          Loading scenarios...
        </p>
      )}

      {!loading && scenarios.length === 0 && (
        <p className="mt-3 text-sm text-gray-500">
          No saved scenarios yet.
        </p>
      )}

      {/* Telegram connection */}

      <div className="mt-6 border-t pt-5">
        <h3 className="font-semibold">
          Telegram Connection
        </h3>

        <p className="mt-2 text-sm text-gray-600">
          Link your Telegram account to Pacey32 to receive
          personalised selections and results when the
          notification service is enabled.
        </p>

        <div className="mt-4 flex flex-wrap items-center gap-3">
          {/* Connection status */}

          <span
            className={`rounded-lg border px-3 py-2 text-sm font-medium ${
              connectionStatus === "connected"
                ? "border-green-700 bg-green-50 text-green-800"
                : "border-gray-300 bg-gray-50 text-gray-600"
            }`}
          >
            {connectionStatus === "loading"
              ? "Checking connection..."
              : connectionStatus === "connected"
                ? "Telegram Connected"
                : "Not Connected"}
          </span>

          {/* Connect button */}

          {connectionStatus === "disconnected" && (
            <button
              type="button"
              onClick={() =>
                void handleConnectTelegram()
              }
              disabled={connectionBusy}
              className="rounded-lg bg-[#123d2d] px-4 py-2 text-sm font-semibold text-white disabled:opacity-40"
            >
              {connectionBusy
                ? "Generating link..."
                : telegramLink
                  ? "Generate New Link"
                  : "Connect Telegram"}
            </button>
          )}

          {/* Check connection button */}

          <button
            type="button"
            onClick={() =>
              void handleCheckConnection()
            }
            disabled={
              connectionBusy ||
              connectionStatus === "loading"
            }
            className="rounded-lg border border-gray-300 bg-white px-4 py-2 text-sm font-semibold text-gray-700 disabled:opacity-40"
          >
            {connectionBusy
              ? "Checking..."
              : "Check Connection"}
          </button>

          {/* Disconnect button */}

          {connectionStatus === "connected" && (
            <button
              type="button"
              onClick={() =>
                void handleDisconnectTelegram()
              }
              disabled={connectionBusy}
              className="rounded-lg border border-red-300 bg-white px-4 py-2 text-sm font-semibold text-red-700 hover:bg-red-50 disabled:opacity-40"
            >
              {connectionBusy
                ? "Disconnecting..."
                : "Disconnect Telegram"}
            </button>
          )}
        </div>

        {/* Telegram linking URL */}

        {telegramLink &&
          connectionStatus !== "connected" && (
            <div className="mt-4 space-y-2">
              <a
                href={telegramLink}
                target="_blank"
                rel="noopener noreferrer"
                referrerPolicy="no-referrer"
                className="inline-flex rounded-lg bg-[#229ED9] px-4 py-2 text-sm font-semibold text-white"
              >
                Open @Pacey32HorseAlerts_bot
              </a>

              <p className="text-xs text-gray-500">
                In Telegram, press Start to link your
                account. Return here and select
                Check Connection.
              </p>
            </div>
          )}

        {/* Connection feedback */}

        {connectionMessage && (
          <p
            role="status"
            className="mt-3 text-sm text-gray-600"
          >
            {connectionMessage}
          </p>
        )}

        {/* Connected status explanation */}

        {connectionStatus === "connected" && (
          <p className="mt-3 text-xs text-gray-500">
            Your Telegram account is linked.
            Personalised notifications will use
            your enabled scenario preferences
            once delivery is configured.
          </p>
        )}

        {connectionStatus === "disconnected" && (
          <p className="mt-3 text-xs text-gray-500">
            You can connect your Telegram account
            to receive personalised racing alerts.
            Disconnecting does not delete your
            saved scenarios or preferences.
          </p>
        )}
      </div>

      {/* General errors */}

      {error && (
        <p
          role="alert"
          className="mt-4 text-sm text-red-700"
        >
          {error}
        </p>
      )}
    </section>
  );
}
