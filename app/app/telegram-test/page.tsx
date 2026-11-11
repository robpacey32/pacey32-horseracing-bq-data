
"use client";

import { useState } from "react";
import { auth } from "@/lib/firebase";

export default function TelegramTestPage() {
  const [result, setResult] = useState("");

  async function testLinking() {
    try {
      const user = auth.currentUser;

      if (!user) {
        setResult("Not signed in");
        return;
      }

      const token = await user.getIdToken();

      const response = await fetch("/api/telegram/link-code", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      const data = await response.json();

      setResult(
        response.ok
          ? `Send this message to @Pacey32HorseAlerts_bot:\n\n/start ${data.code}`
          : `Error ${response.status}: ${data.error}`
      );
    } catch (error) {
      setResult(String(error));
    }
  }

  return (
    <main className="p-8">
      <h1 className="mb-4 text-xl font-bold">
        Telegram Linking Test
      </h1>

      <button
        onClick={testLinking}
        className="rounded bg-green-800 px-4 py-2 text-white"
      >
        Test Linking
      </button>

      <p className="mt-4 whitespace-pre-wrap break-all">{result}</p>
    </main>
  );
}
