import { createHash } from "node:crypto";
import { BigQuery } from "@google-cloud/bigquery";
import {
  applicationDefault,
  cert,
  initializeApp,
} from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import nextEnv from "@next/env";

const { loadEnvConfig } = nextEnv;

const PROJECT = "horseracing-pacey32-github";

const DELIVERIES =
  "`horseracing-pacey32-github.bettingalerts." +
  "UserScenarioAlertDeliveries`";

const RESULTS =
  "`horseracing-pacey32-github.bettingalerts." +
  "UserScenarioSelectionResults_vw`";

function hash(value) {
  return createHash("sha256")
    .update(JSON.stringify(value))
    .digest("hex");
}

function londonDate(now) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/London",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);

  const part = (type) =>
    parts.find((item) => item.type === type)?.value ?? "";

  return `${part("year")}-${part("month")}-${part("day")}`;
}

function money(value) {
  return Number(value).toLocaleString("en-GB", {
    style: "currency",
    currency: "GBP",
  });
}

function singleLine(value) {
  return String(value).replace(/[\r\n\t]+/g, " ").trim();
}

function readOptions() {
  const args = process.argv.slice(2);

  let uid = null;
  let allAccounts = false;
  let test = false;
  let write = false;

  for (let index = 0; index < args.length; index++) {
    const arg = args[index];

    if (arg === "--uid") {
      if (uid !== null) {
        throw new Error("Specify --uid only once.");
      }

      uid = args[++index];

      if (
        !uid ||
        uid.startsWith("--") ||
        uid.includes("/") ||
        uid.length > 128
      ) {
        throw new Error("Invalid Firebase UID.");
      }
    } else if (arg === "--all-accounts") {
      allAccounts = true;
    } else if (arg === "--test") {
      test = true;
    } else if (arg === "--write") {
      write = true;
    } else {
      throw new Error(`Unknown option: ${arg}`);
    }
  }

  if ((!uid && !allAccounts) || (uid && allAccounts)) {
    throw new Error("Choose --uid YOUR_UID or --all-accounts.");
  }

  if (test && allAccounts) {
    throw new Error("Test mode requires one explicit --uid.");
  }

  return { uid, allAccounts, test, write };
}

function renderMessage(name, rows, test) {
  const heading =
    (test ? "🧪 TEST MESSAGE — NOT AN EVENING ALERT\n\n" : "") +
    `📊 Today's ${singleLine(name)} Results\n\n`;

  const lines = rows.map((row) => {
    const result =
      row.SettlementStatus === "VOID"
        ? "Void"
        : row.RawPosition;

    return (
      `${String(row.RaceTime).slice(0, 5)} ` +
      `${singleLine(row.RaceLocation)} - ` +
      `${singleLine(row.HorseName)} | ${singleLine(result)}`
    );
  });

  return heading + lines.join("\n");
}

function splitMessages(name, rows, test, awaiting, review) {
  const chunks = [];
  let chunk = [];

  for (const row of rows) {
    const candidate = [...chunk, row];

    if (
      renderMessage(
        name,
        candidate,
        test,
        awaiting,
        review
      ).length > 3800
    ) {
      if (chunk.length === 0) {
        throw new Error("A result is too long for Telegram.");
      }

      chunks.push(chunk);
      chunk = [row];

      if (
        renderMessage(
          name,
          chunk,
          test,
          awaiting,
          review
        ).length > 3800
      ) {
        throw new Error("A result is too long for Telegram.");
      }
    } else {
      chunk = candidate;
    }
  }

  if (chunk.length > 0) chunks.push(chunk);

  return chunks.map((items) => ({
    text: renderMessage(
      name,
      items,
      test,
      awaiting,
      review
    ),
    selectionIDs: items.map((row) => row.SelectionID),
  }));
}

async function buildPlans(context, uid, morning) {
  const { bigquery, firestore, date, options } = context;

  const [resultsResponse, priorResponse, link] =
    await Promise.all([
      bigquery.query({
        query: `
          SELECT
            SelectionID,
            ScenarioID,
            ScenarioName,
            RaceTime,
            RaceLocation,
            HorseName,
            RawPosition,
            SettlementStatus,
            Staked,
            Returns,
            Profit,
            RefundedStake
          FROM ${RESULTS}
          WHERE SelectionDate = DATE(@date)
            AND FirebaseUID = @uid
            AND IsTest = @isTest
          ORDER BY RaceTime, RaceLocation, HorseName
        `,
        params: { date, uid, isTest: options.test },
        maximumBytesBilled: "10000000000",
      }),

      bigquery.query({
        query: `
          SELECT
            ScenarioID,
            PartNumber,
            SelectionIDs,
            DeliveryStatus
          FROM ${DELIVERIES}
          WHERE AlertDate = DATE(@date)
            AND FirebaseUID = @uid
            AND IsTest = @isTest
            AND AlertType = 'EVENING'
        `,
        params: { date, uid, isTest: options.test },
        maximumBytesBilled: "1000000000",
      }),

      firestore
        .collection("telegramAccountLinks")
        .doc(uid)
        .get(),
    ]);

  const [resultRows] = resultsResponse;
  const [prior] = priorResponse;

  // Any existing receipt reserves its selections, including
  // pending or uncertain deliveries. Never blindly repeat them.
  const reservedIDs = new Set(
    prior.flatMap((row) => row.SelectionIDs ?? [])
  );

  const seenResults = new Set();

  for (const row of resultRows) {
    if (
      !row.SelectionID ||
      seenResults.has(row.SelectionID)
    ) {
      throw new Error("Missing or duplicate result SelectionID.");
    }

    seenResults.add(row.SelectionID);
  }

  const scenarios = new Map();

  for (const row of morning) {
    if (!scenarios.has(row.ScenarioID)) {
      scenarios.set(row.ScenarioID, {
        id: row.ScenarioID,
        name: row.ScenarioName,
        selectionIDs: new Set(),
      });
    }

    for (const id of row.SelectionIDs ?? []) {
      scenarios.get(row.ScenarioID).selectionIDs.add(id);
    }
  }

  const deliveries = [];

  for (const scenario of [...scenarios.values()].sort(
    (a, b) =>
      String(a.name).localeCompare(
        String(b.name),
        "en-GB",
        { numeric: true }
      )
  )) {
    if (scenario.selectionIDs.size === 0) {
      console.log(
        `${scenario.name}: no morning selections; ` +
        "no evening message."
      );
      continue;
    }

    const currentScenario = await firestore
      .collection("users")
      .doc(uid)
      .collection("scenarios")
      .doc(scenario.id)
      .get();

    if (
      !currentScenario.exists ||
      currentScenario.get("notificationsEnabled") !== true
    ) {
      console.log(
        `${scenario.name}: disabled or deleted; skipped.`
      );
      continue;
    }

    const rows = resultRows.filter(
      (row) =>
        row.ScenarioID === scenario.id &&
        scenario.selectionIDs.has(row.SelectionID)
    );

    const awaiting = rows.filter(
      (row) => row.SettlementStatus === "AWAITING_RESULT"
    ).length;

    const review = rows.filter(
      (row) => row.SettlementStatus === "NEEDS_REVIEW"
    ).length;

    const eligible = rows.filter(
      (row) =>
        ["SETTLED", "VOID"].includes(
          row.SettlementStatus
        ) &&
        !reservedIDs.has(row.SelectionID)
    );

    console.log(
      `${scenario.name}: ${eligible.length} new results, ` +
      `${awaiting} awaiting results, ${review} needing review.`
    );

    if (eligible.length === 0) continue;

    if (!link.exists) {
      console.log("No current Telegram connection; skipped.");
      continue;
    }

    const chatID = String(link.get("chatID") ?? "");

    if (!/^\d+$/.test(chatID)) {
      throw new Error("Invalid private Telegram connection.");
    }

    const previousPart = Math.max(
      0,
      ...prior
        .filter((row) => row.ScenarioID === scenario.id)
        .map((row) => Number(row.PartNumber))
    );

    const messages = splitMessages(
      scenario.name,
      eligible,
      options.test,
      awaiting,
      review
    );

    messages.forEach((message, index) => {
      deliveries.push({
        DeliveryID: hash([
          uid,
          date,
          scenario.id,
          "EVENING",
          options.test,
          [...message.selectionIDs].sort(),
        ]),
        AlertDate: date,
        FirebaseUID: uid,
        ScenarioID: scenario.id,
        ScenarioName: scenario.name,
        AlertType: "EVENING",
        IsTest: options.test,
        PartNumber: previousPart + index + 1,
        ChatID: chatID,
        MessageText: message.text,
        SelectionIDs: message.selectionIDs,
        DeliveryStatus: "PENDING",
        CreatedAt: new Date().toISOString(),
        SentAt: null,
        TelegramMessageID: null,
        ErrorMessage: null,
      });
    });
  }

  return deliveries;
}

async function writePlans(context, deliveries) {
  if (deliveries.length === 0) return;

  const json = JSON.stringify(deliveries);

  if (Buffer.byteLength(json, "utf8") > 8000000) {
    throw new Error("The results batch is too large.");
  }

  await context.bigquery.query({
    query: `
      MERGE ${DELIVERIES} T
      USING (
        SELECT
          JSON_VALUE(record, '$.DeliveryID') AS DeliveryID,
          CAST(
            JSON_VALUE(record, '$.AlertDate') AS DATE
          ) AS AlertDate,
          JSON_VALUE(record, '$.FirebaseUID') AS FirebaseUID,
          JSON_VALUE(record, '$.ScenarioID') AS ScenarioID,
          JSON_VALUE(record, '$.ScenarioName') AS ScenarioName,
          JSON_VALUE(record, '$.AlertType') AS AlertType,
          CAST(
            JSON_VALUE(record, '$.IsTest') AS BOOL
          ) AS IsTest,
          CAST(
            JSON_VALUE(record, '$.PartNumber') AS INT64
          ) AS PartNumber,
          JSON_VALUE(record, '$.ChatID') AS ChatID,
          JSON_VALUE(record, '$.MessageText') AS MessageText,
          ARRAY(
            SELECT JSON_VALUE(item)
            FROM UNNEST(
              JSON_QUERY_ARRAY(record, '$.SelectionIDs')
            ) item
          ) AS SelectionIDs,
          JSON_VALUE(
            record, '$.DeliveryStatus'
          ) AS DeliveryStatus,
          CAST(
            JSON_VALUE(record, '$.CreatedAt') AS TIMESTAMP
          ) AS CreatedAt
        FROM UNNEST(JSON_QUERY_ARRAY(@records)) record
      ) S
      ON T.DeliveryID = S.DeliveryID

      WHEN NOT MATCHED THEN
        INSERT (
          DeliveryID, AlertDate, FirebaseUID,
          ScenarioID, ScenarioName, AlertType,
          IsTest, PartNumber, ChatID, MessageText,
          SelectionIDs, DeliveryStatus, CreatedAt,
          SentAt, TelegramMessageID, ErrorMessage
        )
        VALUES (
          S.DeliveryID, S.AlertDate, S.FirebaseUID,
          S.ScenarioID, S.ScenarioName, S.AlertType,
          S.IsTest, S.PartNumber, S.ChatID, S.MessageText,
          S.SelectionIDs, S.DeliveryStatus, S.CreatedAt,
          NULL, NULL, NULL
        )
    `,
    params: { records: json },
    maximumBytesBilled: "10000000000",
  });
}

async function processAccount(context, uid, morning) {
  const { firestore, options, date } = context;

  console.log(`\nACCOUNT: ${uid}`);

  let lockRef = null;
  let claimed = false;
  let committed = false;

  try {
    if (options.write) {
      lockRef = firestore
        .collection("scenarioAlertGenerationLocks")
        .doc(hash([uid, date, "EVENING", options.test]));

      await firestore.runTransaction(async (transaction) => {
        const lock = await transaction.get(lockRef);

        if (
          lock.exists &&
          lock.get("status") !== "COMPLETE"
        ) {
          throw new Error(
            "An earlier evening-generation attempt needs review."
          );
        }

        transaction.set(lockRef, {
          firebaseUID: uid,
          date,
          isTest: options.test,
          status: "GENERATING",
          startedAt: new Date().toISOString(),
        });
      });

      claimed = true;
    }

    // Read again under the reservation in write mode.
    // Previously queued results are excluded.
    const deliveries = await buildPlans(
      context,
      uid,
      morning
    );

    for (const delivery of deliveries) {
      console.log(
        `\nDelivery ID: ${delivery.DeliveryID}\n` +
        `Recipient: ${delivery.ChatID}\n\n` +
        delivery.MessageText
      );
    }

    if (options.write) {
      if (londonDate(new Date()) !== date) {
        throw new Error(
          "The London date changed. Run the new day's process."
        );
      }

      await writePlans(context, deliveries);
      committed = true;

      await lockRef.update({
        status: "COMPLETE",
        completedAt: new Date().toISOString(),
        messageCount: deliveries.length,
      });

      console.log(
        `Queued ${deliveries.length} new evening messages.`
      );
    } else {
      console.log(
        `Preview: ${deliveries.length} evening messages. ` +
        "Nothing written or sent."
      );
    }
  } catch (error) {
    if (claimed) {
      try {
        await lockRef.update({
          status: "NEEDS_REVIEW",
          recordsWritten: committed,
          updatedAt: new Date().toISOString(),
        });
      } catch {
        // The existing reservation still blocks concurrent writes.
      }
    }

    throw error;
  }
}

async function main() {
  const options = readOptions();

  loadEnvConfig(process.cwd(), true);

  const credentials =
    process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON
      ? JSON.parse(
          process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON
        )
      : undefined;

  const firebaseApp = initializeApp({
    projectId: PROJECT,
    credential: credentials
      ? cert(credentials)
      : applicationDefault(),
  });

  const firestore = getFirestore(firebaseApp);

  const bigquery = new BigQuery({
    projectId: PROJECT,
    ...(credentials ? { credentials } : {}),
  });

  const date = londonDate(new Date());

  console.log(
    options.write
      ? "EVENING RESULTS — QUEUE WRITES ENABLED"
      : "EVENING RESULTS — READ-ONLY PREVIEW"
  );

  console.log(`London date: ${date}`);
  console.log(`Test batch: ${options.test}`);
  console.log("This script never sends Telegram messages.\n");

  const [morning] = await bigquery.query({
    query: `
      SELECT
        FirebaseUID,
        ScenarioID,
        ScenarioName,
        SelectionIDs
      FROM ${DELIVERIES}
      WHERE AlertDate = DATE(@date)
        AND AlertType = 'MORNING'
        AND DeliveryStatus = 'SENT'
        AND IsTest = @isTest
        AND (@uid = '' OR FirebaseUID = @uid)
      ORDER BY FirebaseUID, ScenarioName, PartNumber
    `,
    params: {
      date,
      isTest: options.test,
      uid: options.uid ?? "",
    },
    maximumBytesBilled: "1000000000",
  });

  const accounts = new Map();

  for (const row of morning) {
    if (!accounts.has(row.FirebaseUID)) {
      accounts.set(row.FirebaseUID, []);
    }

    accounts.get(row.FirebaseUID).push(row);
  }

  const context = {
    firestore,
    bigquery,
    options,
    date,
  };

  let failures = 0;

  for (const [uid, receipts] of accounts) {
    try {
      await processAccount(context, uid, receipts);
    } catch (error) {
      failures++;

      console.error(
        `Account ${uid} failed:`,
        error instanceof Error ? error.message : String(error)
      );
    }
  }

  console.log(
    `\nFinished: ${accounts.size} accounts checked, ` +
    `${failures} failures. No messages sent.`
  );

  if (failures > 0) process.exitCode = 1;
}

main().catch((error) => {
  console.error(
    "Results generation failed:",
    error instanceof Error ? error.message : String(error)
  );

  process.exitCode = 1;
});