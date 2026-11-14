import { createHash } from "node:crypto";
import { BigQuery } from "@google-cloud/bigquery";
import {
  applicationDefault,
  cert,
  initializeApp,
} from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import nextEnv from "@next/env";

import { scenarioWhere } from "../lib/scenarios/queryFilters.ts";

const { loadEnvConfig } = nextEnv;

const PROJECT = "horseracing-pacey32-github";

const SOURCE =
  "`horseracing-pacey32-github.horseraceanalytics." +
  "Scenario_PreRace_Data_vw`";

const SELECTIONS =
  "`horseracing-pacey32-github.bettingalerts." +
  "UserScenarioSelections`";

const DELIVERIES =
  "`horseracing-pacey32-github.bettingalerts." +
  "UserScenarioAlertDeliveries`";

const SELECTION_COLUMNS = [
  ["SelectionDate", "DATE"],
  ["FirebaseUID", "STRING"],
  ["ScenarioID", "STRING"],
  ["ScenarioName", "STRING"],
  ["RaceTime", "STRING"],
  ["RaceLocation", "STRING"],
  ["HorseName", "STRING"],
  ["OddsDecimal", "FLOAT64"],
  ["CreatedAt", "TIMESTAMP"],
  ["SelectionID", "STRING"],
  ["RaceSourceURL", "STRING"],
  ["BetType", "STRING"],
  ["WinStake", "FLOAT64"],
  ["PlaceStake", "FLOAT64"],
  ["PlacesPaid", "INT64"],
  ["PlaceTerms", "FLOAT64"],
  ["DeliveryStatus", "STRING"],
  ["SentAt", "TIMESTAMP"],
  ["TelegramMessageID", "INT64"],
  ["ScenarioVersion", "STRING"],
  ["ScenarioFiltersJSON", "STRING"],
];

const DELIVERY_COLUMNS = [
  ["DeliveryID", "STRING"],
  ["AlertDate", "DATE"],
  ["FirebaseUID", "STRING"],
  ["ScenarioID", "STRING"],
  ["ScenarioName", "STRING"],
  ["AlertType", "STRING"],
  ["IsTest", "BOOL"],
  ["PartNumber", "INT64"],
  ["ChatID", "STRING"],
  ["MessageText", "STRING"],
  ["SelectionIDs", "ARRAY"],
  ["DeliveryStatus", "STRING"],
  ["CreatedAt", "TIMESTAMP"],
  ["SentAt", "TIMESTAMP"],
  ["TelegramMessageID", "INT64"],
  ["ErrorMessage", "STRING"],
];

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

function singleLine(value) {
  return String(value).replace(/[\r\n\t]+/g, " ").trim();
}

function fractionalOdds(value) {
  const profit = Number(value) - 1;

  if (!Number.isFinite(profit) || profit <= 0) {
    throw new Error(`Invalid decimal odds: ${value}`);
  }

  for (let denominator = 1; denominator <= 1000; denominator++) {
    const numerator = Math.round(profit * denominator);

    if (
      numerator > 0 &&
      Math.abs(profit - numerator / denominator) < 0.00000001
    ) {
      return `${numerator}/${denominator}`;
    }
  }

  throw new Error(`Cannot convert decimal odds ${value}.`);
}

function canonical(value) {
  if (Array.isArray(value)) {
    return [...new Set(value.map((item) =>
      typeof item === "string"
        ? item.trim().toLowerCase()
        : item
    ))].sort();
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, item]) => item !== undefined)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([key, item]) => [key, canonical(item)])
    );
  }

  return typeof value === "string"
    ? value.trim().toLowerCase()
    : value;
}

function readOptions() {
  const args = process.argv.slice(2);
  let uid = null;
  let allAccounts = false;
  let write = false;
  let morning = false;
  let test = false;

  for (let index = 0; index < args.length; index++) {
    const arg = args[index];

    if (arg === "--uid") {
      if (uid !== null) throw new Error("Specify --uid only once.");
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
    } else if (arg === "--write") {
      write = true;
    } else if (arg === "--morning") {
      morning = true;
    } else if (arg === "--test") {
      test = true;
    } else {
      throw new Error(`Unknown option: ${arg}`);
    }
  }

  if ((!uid && !allAccounts) || (uid && allAccounts)) {
    throw new Error("Choose --uid YOUR_UID or --all-accounts.");
  }

  if (test && (allAccounts || morning)) {
    throw new Error("Use --test with one --uid and without --morning.");
  }

  if (write && !morning && !test) {
    throw new Error(
      "Production writes require --morning. " +
      "For a single-account test batch, use --test --write."
    );
  }

  return { uid, allAccounts, write, morning, test };
}

function validateScenario(data, id) {
  const name = singleLine(data.name || id);
  const stakeMode = data.stakeMode ?? "total";

  if (
    typeof data.stake !== "number" ||
    !Number.isFinite(data.stake) ||
    data.stake <= 0 ||
    data.stake > 100000 ||
    !["win", "each_way"].includes(data.betType) ||
    !["per_horse", "per_race"].includes(data.stakeAllocation) ||
    !["total", "per_leg"].includes(stakeMode) ||
    !data.filters ||
    typeof data.filters !== "object" ||
    Array.isArray(data.filters)
  ) {
    throw new Error(`Invalid saved settings in "${name}".`);
  }

  const filtersJSON = JSON.stringify(data.filters);

  return {
    id,
    name,
    stake: data.stake,
    betType: data.betType,
    stakeAllocation: data.stakeAllocation,
    stakeMode,
    filters: data.filters,
    filtersJSON,
    version: hash({
      filters: canonical(data.filters),
      stake: data.stake,
      betType: data.betType,
      stakeAllocation: data.stakeAllocation,
      stakeMode,
    }),
  };
}

function buildMessages(scenario, records, test) {
  const heading =
    (test ? "🧪 TEST MESSAGE — NOT A MORNING ALERT\n\n" : "") +
    `🐎 Today's ${scenario.name} Picks\n\n`;

  if (records.length === 0) {
    return [{
      text: heading + "No selections today.",
      selectionIDs: [],
    }];
  }

  const messages = [];
  let text = heading;
  let selectionIDs = [];

  for (const record of records) {
    const line =
      `${record.RaceTime.slice(0, 5)} ` +
      `${singleLine(record.RaceLocation)} - ` +
      `${singleLine(record.HorseName)} ` +
      `(${fractionalOdds(record.OddsDecimal)})\n`;

    if (heading.length + line.length > 3800) {
      throw new Error("A selection is too long for Telegram.");
    }

    if (text.length + line.length > 3800) {
      messages.push({ text: text.trimEnd(), selectionIDs });
      text = heading;
      selectionIDs = [];
    }

    text += line;
    selectionIDs.push(record.SelectionID);
  }

  messages.push({ text: text.trimEnd(), selectionIDs });
  return messages;
}

function jsonSource(columns, parameter) {
  const expressions = columns.map(([name, type]) => {
    if (type === "ARRAY") {
      return `
        ARRAY(
          SELECT JSON_VALUE(item)
          FROM UNNEST(
            JSON_QUERY_ARRAY(record, '$.${name}')
          ) item
        ) AS \`${name}\`
      `;
    }

    const value = `JSON_VALUE(record, '$.${name}')`;

    return type === "STRING"
      ? `${value} AS \`${name}\``
      : `CAST(${value} AS ${type}) AS \`${name}\``;
  });

  return `
    SELECT ${expressions.join(",\n")}
    FROM UNNEST(JSON_QUERY_ARRAY(@${parameter})) record
  `;
}

function insertMissingSQL(table, columns, parameter, id) {
  return `
    MERGE ${table} T
    USING (${jsonSource(columns, parameter)}) S
    ON T.\`${id}\` = S.\`${id}\`
    WHEN NOT MATCHED THEN
      INSERT (
        ${columns.map(([name]) => `\`${name}\``).join(", ")}
      )
      VALUES (
        ${columns.map(([name]) => `S.\`${name}\``).join(", ")}
      );
  `;
}

async function inspectSource(bigquery, date, morning) {
  const [rows] = await bigquery.query({
    query: `
      SELECT
        COUNT(*) AS RunnerCount,
        COUNTIF(
          Pre_load_timestamp IS NULL
          OR TIMESTAMP(Pre_load_timestamp)
             < TIMESTAMP(DATE(@date), 'Europe/London')
        ) AS StaleRows,
        UNIX_MILLIS(MIN(RaceDateTime)) AS FirstRaceMillis
      FROM ${SOURCE}
      WHERE RaceDateDt = DATE(@date)
    `,
    params: { date },
    maximumBytesBilled: "10000000000",
  });

  const row = rows[0];
  const runnerCount = Number(row?.RunnerCount ?? 0);
  const firstRaceMillis = Number(row?.FirstRaceMillis);

  console.log(`Available priced runners: ${runnerCount}`);

  if (morning) {
    if (
      runnerCount === 0 ||
      Number(row?.StaleRows ?? 0) > 0 ||
      !Number.isFinite(firstRaceMillis) ||
      firstRaceMillis <= Date.now()
    ) {
      throw new Error(
        "Morning generation requires today's fresh racecards " +
        "and must run before the first race. Nothing written."
      );
    }
  }

  return firstRaceMillis;
}

async function writeBatch(
  context,
  account,
  records,
  deliveries
) {
  const {
    firestore,
    bigquery,
    options,
    date,
    firstRaceMillis,
  } = context;

  if (
    londonDate(new Date()) !== date ||
    (options.morning && Date.now() >= firstRaceMillis)
  ) {
    throw new Error("The morning generation window has passed.");
  }

  const selectionJSON = JSON.stringify(records);
  const deliveryJSON = JSON.stringify(deliveries);

  if (
    Buffer.byteLength(selectionJSON, "utf8") +
      Buffer.byteLength(deliveryJSON, "utf8") > 8000000
  ) {
    throw new Error("This account's batch is too large.");
  }

  const lockID = hash([
    account.uid,
    date,
    "MORNING",
    options.test,
  ]);

  const lockRef = firestore
    .collection("scenarioAlertGenerationLocks")
    .doc(lockID);

  // This is an internal run reservation, not a user save feature.
  const claimed = await firestore.runTransaction(
    async (transaction) => {
      const [lock, currentUser, currentLink] = await Promise.all([
        transaction.get(lockRef),
        transaction.get(
          firestore.collection("users").doc(account.uid)
        ),
        transaction.get(
          firestore
            .collection("telegramAccountLinks")
            .doc(account.uid)
        ),
      ]);

      if (lock.exists) {
        if (lock.get("status") === "COMPLETE") return false;

        throw new Error(
          "An earlier generation attempt needs review. " +
          "It will not be repeated automatically."
        );
      }

      if (
        (currentUser.get("notificationSettingsVersion") ?? 0)
          !== account.settingsVersion ||
        String(currentLink.get("chatID")) !== account.chatID ||
        String(currentLink.get("telegramUserID"))
          !== account.telegramUserID
      ) {
        throw new Error(
          "Account settings or its Telegram connection changed. " +
          "Run the preview again."
        );
      }

      transaction.create(lockRef, {
        firebaseUID: account.uid,
        date,
        isTest: options.test,
        status: "GENERATING",
        startedAt: new Date().toISOString(),
      });

      return true;
    }
  );

  if (!claimed) {
    console.log("Already generated for this account/date/mode.");
    return;
  }

  let committed = false;

  try {
    const [existing] = await bigquery.query({
      query: `
        SELECT COUNT(*) AS DeliveryCount
        FROM ${DELIVERIES}
        WHERE FirebaseUID = @uid
          AND AlertDate = DATE(@date)
          AND AlertType = 'MORNING'
          AND IsTest = @isTest
      `,
      params: {
        uid: account.uid,
        date,
        isTest: options.test,
      },
      maximumBytesBilled: "1000000000",
    });

    if (Number(existing[0]?.DeliveryCount ?? 0) > 0) {
      throw new Error(
        "Delivery records already exist for this batch. " +
        "Review them before regenerating."
      );
    }

    // Refresh only unsent rows of the matching mode.
    // Already sent/test-sent records remain untouched.
    const pendingStatus = options.test
      ? "TEST_PENDING"
      : "PENDING";

    await bigquery.query({
      query: `
        BEGIN TRANSACTION;

        DELETE FROM ${SELECTIONS}
        WHERE FirebaseUID = @uid
          AND SelectionDate = DATE(@date)
          AND DeliveryStatus = @pendingStatus
          AND SentAt IS NULL
          AND TelegramMessageID IS NULL;

        ${insertMissingSQL(
          SELECTIONS,
          SELECTION_COLUMNS,
          "selections",
          "SelectionID"
        )}

        ${insertMissingSQL(
          DELIVERIES,
          DELIVERY_COLUMNS,
          "deliveries",
          "DeliveryID"
        )}

        COMMIT TRANSACTION;
      `,
      params: {
        uid: account.uid,
        date,
        pendingStatus,
        selections: selectionJSON,
        deliveries: deliveryJSON,
      },
      maximumBytesBilled: "10000000000",
    });

    committed = true;

    await lockRef.update({
      status: "COMPLETE",
      completedAt: new Date().toISOString(),
      selectionCount: records.length,
      messageCount: deliveries.length,
    });

    console.log(
      `Written: ${records.length} selections and ` +
      `${deliveries.length} pending messages.`
    );
  } catch (error) {
    try {
      await lockRef.update({
        status: "NEEDS_REVIEW",
        recordsWritten: committed,
        updatedAt: new Date().toISOString(),
      });
    } catch {
      // The existing reservation still prevents automatic repetition.
    }

    throw error;
  }
}

async function generateAccount(context, uid) {
  const { firestore, bigquery, options, date } = context;

  const userRef = firestore.collection("users").doc(uid);

  const [user, link, saved] = await Promise.all([
    userRef.get(),
    firestore
      .collection("telegramAccountLinks")
      .doc(uid)
      .get(),
    userRef
      .collection("scenarios")
      .where("notificationsEnabled", "==", true)
      .limit(11)
      .get(),
  ]);

  console.log(`\nACCOUNT: ${uid}`);

  if (!link.exists) {
    console.log("No Telegram connection; skipped.");
    return;
  }

  if (saved.empty) {
    console.log("No enabled scenarios; skipped.");
    return;
  }

  if (saved.size > 10) {
    throw new Error("More than 10 enabled scenarios.");
  }

  const chatID = String(link.get("chatID") ?? "");
  const telegramUserID = String(
    link.get("telegramUserID") ?? ""
  );

  if (!/^\d+$/.test(chatID) || !/^\d+$/.test(telegramUserID)) {
    throw new Error("Invalid private Telegram connection.");
  }

  const account = {
    uid,
    chatID,
    telegramUserID,
    settingsVersion:
      user.get("notificationSettingsVersion") ?? 0,
  };

  const scenarios = saved.docs
    .map((document) =>
      validateScenario(document.data(), document.id)
    )
    .sort((a, b) =>
      a.name.localeCompare(b.name, "en-GB", { numeric: true })
      || a.id.localeCompare(b.id)
    );

  const records = [];
  const deliveries = [];
  const seen = new Set();

  for (const scenario of scenarios) {
    const { conditions, params } = scenarioWhere(
      scenario.filters,
      date,
      date,
      { requireResults: false }
    );

    const totalStake =
      scenario.betType === "each_way" &&
      scenario.stakeMode === "per_leg"
        ? scenario.stake * 2
        : scenario.stake;

    const divisor =
      scenario.stakeAllocation === "per_race"
        ? "qualifiers"
        : "1";

    const [rows] = await bigquery.query({
      query: `
        WITH matched AS (
          SELECT
            RaceDateDt,
            Pre_RaceTime,
            Pre_RaceLocation,
            Pre_SourceURL,
            HorseName,
            OddsDecimal,
            ew_possible,
            places_paid,
            place_terms,
            COUNT(*) OVER (
              PARTITION BY
                RaceDateDt,
                Pre_RaceLocation,
                Pre_RaceTime
            ) AS qualifiers
          FROM ${SOURCE}
          WHERE ${conditions.join(" AND ")}
        )
        SELECT
          CAST(RaceDateDt AS STRING) AS SelectionDate,
          CAST(Pre_RaceTime AS STRING) AS RaceTime,
          Pre_RaceLocation AS RaceLocation,
          Pre_SourceURL AS RaceSourceURL,
          HorseName,
          OddsDecimal,
          SAFE_DIVIDE(@winStake, ${divisor}) AS WinStake,
          IF(
            @placeStake > 0 AND ew_possible,
            SAFE_DIVIDE(@placeStake, ${divisor}),
            0
          ) AS PlaceStake,
          ew_possible AS EachWayEligible,
          places_paid AS PlacesPaid,
          place_terms AS PlaceTerms
        FROM matched
        ORDER BY Pre_RaceTime, Pre_RaceLocation, HorseName
      `,
      params: {
        ...params,
        winStake:
          scenario.betType === "each_way"
            ? totalStake / 2
            : totalStake,
        placeStake:
          scenario.betType === "each_way"
            ? totalStake / 2
            : 0,
      },
      maximumBytesBilled: "10000000000",
    });

    const scenarioRecords = [];

    for (const row of rows) {
      if (
        !row.RaceSourceURL ||
        !row.HorseName ||
        !row.RaceLocation ||
        !row.RaceTime
      ) {
        throw new Error("A selection has incomplete identity.");
      }

      if (
        scenario.betType === "each_way" &&
        (
          typeof row.EachWayEligible !== "boolean" ||
          (
            row.EachWayEligible &&
            (
              row.PlacesPaid == null ||
              row.PlaceTerms == null
            )
          )
        )
      ) {
        throw new Error(
          `Incomplete each-way data: ${row.HorseName}`
        );
      }

      // The mode prefix separates new production and test records.
      const selectionID = hash([
        options.test ? "test-v2" : "production-v2",
        date,
        uid,
        scenario.id,
        row.RaceSourceURL,
        row.HorseName,
      ]);

      if (seen.has(selectionID)) {
        throw new Error(`Duplicate runner: ${row.HorseName}`);
      }

      seen.add(selectionID);

      scenarioRecords.push({
        SelectionDate: row.SelectionDate,
        FirebaseUID: uid,
        ScenarioID: scenario.id,
        ScenarioName: scenario.name,
        RaceTime: String(row.RaceTime),
        RaceLocation: row.RaceLocation,
        HorseName: row.HorseName,
        OddsDecimal: Number(row.OddsDecimal),
        CreatedAt: new Date().toISOString(),
        SelectionID: selectionID,
        RaceSourceURL: row.RaceSourceURL,
        BetType: scenario.betType,
        WinStake: Number(row.WinStake),
        PlaceStake: Number(row.PlaceStake),
        PlacesPaid:
          row.PlacesPaid == null
            ? null
            : Number(row.PlacesPaid),
        PlaceTerms:
          row.PlaceTerms == null
            ? null
            : Number(row.PlaceTerms),
        DeliveryStatus: options.test
          ? "TEST_PENDING"
          : "PENDING",
        SentAt: null,
        TelegramMessageID: null,
        ScenarioVersion: scenario.version,
        ScenarioFiltersJSON: scenario.filtersJSON,
      });
    }

    records.push(...scenarioRecords);

    const messages = buildMessages(
      scenario,
      scenarioRecords,
      options.test
    );

    messages.forEach((message, index) => {
      deliveries.push({
        DeliveryID: hash([
          uid,
          date,
          scenario.id,
          "MORNING",
          options.test,
          index + 1,
        ]),
        AlertDate: date,
        FirebaseUID: uid,
        ScenarioID: scenario.id,
        ScenarioName: scenario.name,
        AlertType: "MORNING",
        IsTest: options.test,
        PartNumber: index + 1,
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

    console.log(
      `${scenario.name}: ${scenarioRecords.length} horses, ` +
      `${messages.length} messages`
    );
  }

  console.log("\nPROPOSED SELECTIONS");
  console.log(JSON.stringify(records, null, 2));

  console.log("\nPROPOSED MESSAGES");

  for (const delivery of deliveries) {
    console.log(
      `\nRecipient: ${delivery.ChatID}\n` +
      `Delivery ID: ${delivery.DeliveryID}\n\n` +
      delivery.MessageText
    );
  }

  if (options.write) {
    await writeBatch(context, account, records, deliveries);
  } else {
    console.log("\nRead-only: nothing written or sent.");
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
      ? "GENERATION — TABLE WRITES ENABLED"
      : "GENERATION — READ ONLY"
  );

  console.log(`London date: ${date}`);
  console.log(`Test batch: ${options.test}`);
  console.log("This script never sends Telegram messages.\n");

  const firstRaceMillis = await inspectSource(
    bigquery,
    date,
    options.morning
  );

  const targets = options.uid
    ? [options.uid]
    : (
        await firestore
          .collection("telegramAccountLinks")
          .get()
      ).docs.map((document) => document.id).sort();

  const context = {
    firestore,
    bigquery,
    options,
    date,
    firstRaceMillis,
  };

  let failures = 0;

  for (const uid of targets) {
    try {
      await generateAccount(context, uid);
    } catch (error) {
      failures++;
      console.error(
        `Account ${uid} failed:`,
        error instanceof Error ? error.message : String(error)
      );
    }
  }

  console.log(
    `\nFinished: ${targets.length} accounts checked, ` +
    `${failures} failures. No messages sent.`
  );

  if (failures > 0) process.exitCode = 1;
}

main().catch((error) => {
  console.error(
    "Generation failed:",
    error instanceof Error ? error.message : String(error)
  );
  process.exitCode = 1;
});