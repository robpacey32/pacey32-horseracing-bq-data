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

const SELECTIONS =
  "`horseracing-pacey32-github.bettingalerts." +
  "UserScenarioSelections`";

const SOURCE =
  "`horseracing-pacey32-github.horseraceanalytics." +
  "Scenario_PreRace_Data_vw`";

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

function readOptions() {
  const args = process.argv.slice(2);

  let uid = null;
  let allAccounts = false;
  let test = false;
  let sendMode = null;
  let type = "MORNING";

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
    } else if (arg === "--send" || arg === "--send-test") {
      if (sendMode !== null) {
        throw new Error("Choose only one sending option.");
      }

      sendMode = arg;
    } else if (arg === "--type") {
      const value = args[++index]?.toUpperCase();

      if (!["MORNING", "EVENING"].includes(value)) {
        throw new Error("--type must be morning or evening.");
      }

      type = value;
    } else {
      throw new Error(`Unknown option: ${arg}`);
    }
  }

  if ((!uid && !allAccounts) || (uid && allAccounts)) {
    throw new Error("Choose --uid YOUR_UID or --all-accounts.");
  }

  if (sendMode === "--send-test") {
    test = true;
  }

  if (sendMode === "--send" && test) {
    throw new Error(
      "Use --send-test for test messages, not --send."
    );
  }

  if (test && allAccounts) {
    throw new Error("Test mode requires one explicit --uid.");
  }

  return {
    uid,
    allAccounts,
    test,
    send: sendMode !== null,
    type,
  };
}

async function getRecipient(context, uid) {
  const { bigquery, firestore } = context;

  const [links] = await bigquery.query({
    query: `
      SELECT TelegramUserID, ChatID
      FROM
        \`horseracing-pacey32-github.bettingalerts.TelegramAccountLinks\`
      WHERE FirebaseUID = @uid
      LIMIT 2
    `,
    params: { uid },
    maximumBytesBilled: "1000000000",
  });

  if (links.length !== 1) {
    throw new Error(
      "Expected exactly one BigQuery Telegram connection."
    );
  }

  const telegramUserID = String(links[0].TelegramUserID);
  const chatID = String(links[0].ChatID);

  const [forward, reverse] = await Promise.all([
    firestore
      .collection("telegramAccountLinks")
      .doc(uid)
      .get(),

    firestore
      .collection("telegramUserLinks")
      .doc(telegramUserID)
      .get(),
  ]);

  if (
    !forward.exists ||
    !reverse.exists ||
    String(forward.get("telegramUserID")) !== telegramUserID ||
    String(forward.get("chatID")) !== chatID ||
    reverse.get("firebaseUID") !== uid ||
    String(reverse.get("chatID")) !== chatID
  ) {
    throw new Error(
      "Website and BigQuery Telegram connections disagree."
    );
  }

  const [subscriptions] = await bigquery.query({
    query: `
      SELECT
        alerts_enabled,
        (
          snoozed_until IS NOT NULL
          AND snoozed_until > CURRENT_TIMESTAMP()
        ) AS snoozed
      FROM
        \`horseracing-pacey32-github.bettingalerts.TelegramUsers\`
      WHERE user_id = @telegramUserID
        AND chat_id = @chatID
      LIMIT 2
    `,
    params: { telegramUserID, chatID },
    maximumBytesBilled: "1000000000",
  });

  if (subscriptions.length !== 1) {
    throw new Error(
      "Expected exactly one Telegram subscription."
    );
  }

  return {
    telegramUserID,
    chatID,
    allowed:
      subscriptions[0].alerts_enabled === true &&
      subscriptions[0].snoozed !== true,
  };
}

async function checkMorningWindow(context) {
  const { options, bigquery, date } = context;

  if (
    !options.send ||
    options.test ||
    options.type !== "MORNING"
  ) {
    return;
  }

  const [rows] = await bigquery.query({
    query: `
      SELECT
        COUNT(*) AS RunnerCount,
        UNIX_MILLIS(MIN(RaceDateTime)) AS FirstRaceMillis
      FROM ${SOURCE}
      WHERE RaceDateDt = DATE(@date)
    `,
    params: { date },
    maximumBytesBilled: "10000000000",
  });

  const firstRace = Number(rows[0]?.FirstRaceMillis);

  if (
    Number(rows[0]?.RunnerCount ?? 0) === 0 ||
    !Number.isFinite(firstRace) ||
    Date.now() >= firstRace
  ) {
    throw new Error(
      "Production morning messages must be sent before the " +
      "first race, with today's racecards available. " +
      "Use test mode for retrospective testing."
    );
  }

  context.firstRaceMillis = firstRace;
}

async function validateSelections(context, delivery) {
  const ids = delivery.SelectionIDs;

  if (ids.length === 0) {
    if (delivery.AlertType === "EVENING") {
      throw new Error(
        "An evening message must reference selections."
      );
    }

    return;
  }

  const expectedStatus =
    delivery.AlertType === "MORNING"
      ? delivery.IsTest
        ? "TEST_PENDING"
        : "PENDING"
      : delivery.IsTest
        ? "TEST_SENT"
        : "SENT";

  const [rows] = await context.bigquery.query({
    query: `
      SELECT
        COUNT(*) AS RowCount,
        COUNT(DISTINCT SelectionID) AS UniqueCount,
        COUNTIF(
          COALESCE(DeliveryStatus, '') != @expectedStatus
        ) AS WrongStatus
      FROM ${SELECTIONS}
      WHERE FirebaseUID = @uid
        AND ScenarioID = @scenarioID
        AND SelectionDate = DATE(@date)
        AND SelectionID IN UNNEST(@selectionIDs)
    `,
    params: {
      uid: delivery.FirebaseUID,
      scenarioID: delivery.ScenarioID,
      date: context.date,
      selectionIDs: ids,
      expectedStatus,
    },
    maximumBytesBilled: "1000000000",
  });

  const row = rows[0];

  if (
    Number(row.RowCount) !== ids.length ||
    Number(row.UniqueCount) !== ids.length ||
    Number(row.WrongStatus) !== 0
  ) {
    throw new Error(
      "The message's selections are missing, duplicated, " +
      "or have an unexpected delivery status."
    );
  }
}

async function updateDelivery(
  context,
  delivery,
  action,
  receipt = null
) {
  const { bigquery, date } = context;

  const previousStatus =
    action === "claim" ? "PENDING" : "SENDING";

  const params = {
    deliveryID: delivery.DeliveryID,
    uid: delivery.FirebaseUID,
    scenarioID: delivery.ScenarioID,
    date,
    isTest: delivery.IsTest,
    alertType: delivery.AlertType,
    chatID: String(delivery.ChatID),
    previousStatus,
  };

  let messageSet;
  let selectionSet;
  let previousSelectionStatus;

  if (action === "claim") {
    messageSet =
      "DeliveryStatus = 'SENDING', ErrorMessage = NULL";

    params.nextSelectionStatus = delivery.IsTest
      ? "TEST_SENDING"
      : "SENDING";

    previousSelectionStatus = delivery.IsTest
      ? "TEST_PENDING"
      : "PENDING";

    selectionSet =
      "DeliveryStatus = @nextSelectionStatus";
  } else if (action === "sent") {
    messageSet = `
      DeliveryStatus = 'SENT',
      SentAt = TIMESTAMP_SECONDS(@telegramDate),
      TelegramMessageID = @messageID,
      ErrorMessage = NULL
    `;

    params.telegramDate = receipt.telegramDate;
    params.messageID = receipt.messageID;
    params.nextSelectionStatus = delivery.IsTest
      ? "TEST_SENT"
      : "SENT";

    previousSelectionStatus = delivery.IsTest
      ? "TEST_SENDING"
      : "SENDING";

    selectionSet = `
      DeliveryStatus = @nextSelectionStatus,
      SentAt = TIMESTAMP_SECONDS(@telegramDate),
      TelegramMessageID = @messageID
    `;
  } else if (action === "review") {
    messageSet = `
      DeliveryStatus = 'NEEDS_REVIEW',
      ErrorMessage = @errorMessage
    `;

    params.errorMessage =
      "Telegram acceptance was not confirmed. " +
      "Check Telegram before recovery.";

    params.nextSelectionStatus = delivery.IsTest
      ? "TEST_NEEDS_REVIEW"
      : "NEEDS_REVIEW";

    previousSelectionStatus = delivery.IsTest
      ? "TEST_SENDING"
      : "SENDING";

    selectionSet =
      "DeliveryStatus = @nextSelectionStatus";
  } else {
    throw new Error("Unknown delivery update.");
  }

  const updateHorses =
    delivery.AlertType === "MORNING" &&
    delivery.SelectionIDs.length > 0;

  let horseSQL = "";

  if (updateHorses) {
    params.selectionIDs = delivery.SelectionIDs;
    params.previousSelectionStatus = previousSelectionStatus;

    horseSQL = `
      UPDATE ${SELECTIONS}
      SET ${selectionSet}
      WHERE FirebaseUID = @uid
        AND ScenarioID = @scenarioID
        AND SelectionDate = DATE(@date)
        AND SelectionID IN UNNEST(@selectionIDs)
        AND DeliveryStatus = @previousSelectionStatus;

      ASSERT @@row_count = ARRAY_LENGTH(@selectionIDs)
        AS 'Selection delivery state changed; review required.';
    `;
  }

  await bigquery.query({
    query: `
      BEGIN TRANSACTION;

      UPDATE ${DELIVERIES}
      SET ${messageSet}
      WHERE DeliveryID = @deliveryID
        AND FirebaseUID = @uid
        AND ScenarioID = @scenarioID
        AND AlertDate = DATE(@date)
        AND IsTest = @isTest
        AND AlertType = @alertType
        AND ChatID = @chatID
        AND DeliveryStatus = @previousStatus;

      ASSERT @@row_count = 1
        AS 'Message was already claimed or its state changed.';

      ${horseSQL}

      COMMIT TRANSACTION;
    `,
    params,
    maximumBytesBilled: "10000000000",
  });
}

async function sendTelegram(token, chatID, text) {
  try {
    const response = await fetch(
      `https://api.telegram.org/bot${token}/sendMessage`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          chat_id: chatID,
          text,
        }),
        signal: AbortSignal.timeout(30000),
      }
    );

    const body = await response.json();

    if (
      !response.ok ||
      body.ok !== true ||
      !Number.isSafeInteger(body.result?.message_id) ||
      !Number.isSafeInteger(body.result?.date)
    ) {
      throw new Error("Acceptance not confirmed.");
    }

    return {
      messageID: body.result.message_id,
      telegramDate: body.result.date,
    };
  } catch {
    // Do not expose a request URL containing the bot token.
    throw new Error("Telegram acceptance was not confirmed.");
  }
}

async function processAccount(context, uid, deliveries) {
  const { options, firestore, date } = context;

  console.log(`\nACCOUNT: ${uid}`);

  const unresolved = deliveries.filter((delivery) =>
    ["SENDING", "NEEDS_REVIEW"].includes(
      delivery.DeliveryStatus
    )
  );

  const pending = deliveries.filter(
    (delivery) => delivery.DeliveryStatus === "PENDING"
  );

  const sent = deliveries.filter(
    (delivery) => delivery.DeliveryStatus === "SENT"
  );

  console.log(`Previously sent messages: ${sent.length}`);
  console.log(`Pending messages: ${pending.length}`);
  console.log(`Messages needing review: ${unresolved.length}`);

  if (options.send && unresolved.length > 0) {
    throw new Error(
      "An earlier delivery needs review. " +
      "No automatic retry will be attempted."
    );
  }

  if (pending.length === 0) {
    console.log("Nothing pending. No messages or rows changed.");
    return;
  }

  const recipient = await getRecipient(context, uid);

  console.log(`Telegram user: ${recipient.telegramUserID}`);
  console.log(`Recipient chat: ${recipient.chatID}`);

  if (!recipient.allowed) {
    console.log("Alerts stopped or snoozed; account skipped.");
    return;
  }

  const seenSelectionIDs = new Set();
  const plan = [];

  for (const delivery of pending) {
    if (String(delivery.ChatID) !== recipient.chatID) {
      throw new Error(
        "The queued recipient differs from the current connection."
      );
    }

    if (
      !Array.isArray(delivery.SelectionIDs) ||
      delivery.SelectionIDs.some(
        (id) => typeof id !== "string" || !id
      )
    ) {
      throw new Error("Invalid selection IDs in a queued message.");
    }

    if (
      typeof delivery.MessageText !== "string" ||
      !delivery.MessageText.trim() ||
      delivery.MessageText.length > 4000
    ) {
      throw new Error("Invalid queued message text.");
    }

    if (
      options.test &&
      !delivery.MessageText.includes("TEST MESSAGE")
    ) {
      throw new Error("A test message is missing its test label.");
    }

    for (const id of delivery.SelectionIDs) {
      if (seenSelectionIDs.has(id)) {
        throw new Error(
          "A selection appears in more than one pending message."
        );
      }

      seenSelectionIDs.add(id);
    }

    const scenario = await firestore
      .collection("users")
      .doc(uid)
      .collection("scenarios")
      .doc(delivery.ScenarioID)
      .get();

    if (
      !scenario.exists ||
      scenario.get("notificationsEnabled") !== true
    ) {
      console.log(
        `${delivery.ScenarioName}: disabled or deleted; skipped.`
      );
      continue;
    }

    await validateSelections(context, delivery);
    plan.push(delivery);
  }

  console.log(`Messages ready: ${plan.length}`);

  for (const delivery of plan) {
    console.log(
      `\n--- ${delivery.ScenarioName}, part ` +
      `${delivery.PartNumber} ---\n`
    );

    console.log(delivery.MessageText);

    if (!options.send) continue;

    if (londonDate(new Date()) !== date) {
      throw new Error("The London date changed. Stopping.");
    }

    if (
      context.firstRaceMillis &&
      Date.now() >= context.firstRaceMillis
    ) {
      throw new Error(
        "The first race has started. Stopping morning delivery."
      );
    }

    const currentRecipient = await getRecipient(context, uid);

    if (
      !currentRecipient.allowed ||
      currentRecipient.chatID !== recipient.chatID ||
      currentRecipient.telegramUserID !== recipient.telegramUserID
    ) {
      throw new Error(
        "The recipient or subscription changed. Nothing further sent."
      );
    }

    const currentScenario = await firestore
      .collection("users")
      .doc(uid)
      .collection("scenarios")
      .doc(delivery.ScenarioID)
      .get();

    if (
      !currentScenario.exists ||
      currentScenario.get("notificationsEnabled") !== true
    ) {
      console.log("Scenario disabled; message skipped.");
      continue;
    }

    // Reserve the message row even when it has zero selections.
    await updateDelivery(context, delivery, "claim");

    let receipt;

    try {
      receipt = await sendTelegram(
        context.token,
        recipient.chatID,
        delivery.MessageText
      );
    } catch {
      try {
        await updateDelivery(context, delivery, "review");
      } catch {
        // SENDING still prevents automatic resending.
      }

      throw new Error(
        "Delivery was not confirmed. Check Telegram and the " +
        "message record before recovery. Do not blindly retry."
      );
    }

    try {
      await updateDelivery(
        context,
        delivery,
        "sent",
        receipt
      );
    } catch {
      throw new Error(
        `Telegram accepted message ${receipt.messageID}, ` +
        "but recording failed. Do not resend it."
      );
    }

    console.log(
      `\nTelegram confirmed message ${receipt.messageID}.`
    );

    console.log(
      options.test
        ? "Message receipt saved with IsTest=true."
        : "Production message receipt saved."
    );
  }
}

async function main() {
  const options = readOptions();

  loadEnvConfig(process.cwd(), true);

  const token = process.env.TELEGRAM_BOT_TOKEN;

  if (options.send && !token) {
    throw new Error(
      "TELEGRAM_BOT_TOKEN is missing. No messages sent."
    );
  }

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
    options.send
      ? "QUEUED TELEGRAM MESSAGES — SENDING ENABLED"
      : "QUEUED TELEGRAM MESSAGES — READ-ONLY PREVIEW"
  );

  console.log(`London date: ${date}`);
  console.log(`Message type: ${options.type}`);
  console.log(`Test messages: ${options.test}`);

  const [rows] = await bigquery.query({
    query: `
      SELECT
        DeliveryID,
        FirebaseUID,
        ScenarioID,
        ScenarioName,
        AlertType,
        IsTest,
        PartNumber,
        ChatID,
        MessageText,
        SelectionIDs,
        DeliveryStatus
      FROM ${DELIVERIES}
      WHERE AlertDate = DATE(@date)
        AND AlertType = @alertType
        AND IsTest = @isTest
        AND (@uid = '' OR FirebaseUID = @uid)
      ORDER BY FirebaseUID, ScenarioName, PartNumber
    `,
    params: {
      date,
      alertType: options.type,
      isTest: options.test,
      uid: options.uid ?? "",
    },
    maximumBytesBilled: "1000000000",
  });

  const seenDeliveryIDs = new Set();
  const accounts = new Map();

  for (const row of rows) {
    if (
      !row.DeliveryID ||
      seenDeliveryIDs.has(row.DeliveryID)
    ) {
      throw new Error(
        "A message has a missing or duplicate DeliveryID."
      );
    }

    seenDeliveryIDs.add(row.DeliveryID);

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
    token,
    firstRaceMillis: null,
  };

  if (rows.some((row) => row.DeliveryStatus === "PENDING")) {
    await checkMorningWindow(context);
  }

  let failures = 0;

  for (const [uid, deliveries] of accounts) {
    try {
      await processAccount(context, uid, deliveries);
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
    `${failures} failures.`
  );

  if (!options.send) {
    console.log("Preview only: no messages or rows changed.");
  }

  if (failures > 0) process.exitCode = 1;
}

main().catch((error) => {
  console.error(
    "Sender failed:",
    error instanceof Error ? error.message : String(error)
  );

  process.exitCode = 1;
});