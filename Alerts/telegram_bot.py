import os
import asyncio
from datetime import datetime, timedelta, timezone

from flask import Flask, request, abort
from google.cloud import bigquery
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

import hashlib
import re
from google.cloud import firestore

# -------------------------
# CONFIG
# -------------------------

PROJECT_ID = "horseracing-pacey32-github"
USERS_TABLE = f"{PROJECT_ID}.bettingalerts.TelegramUsers"

LINKS_TABLE = f"{PROJECT_ID}.bettingalerts.TelegramAccountLinks"
LINK_CODES_COLLECTION = "telegramLinkCodes"
FIREBASE_LINKS_COLLECTION = "telegramAccountLinks"
PENDING_TRANSFERS_COLLECTION = "telegramPendingTransfers"

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_WEBHOOK_SECRET = os.environ["TELEGRAM_WEBHOOK_SECRET"]

app = Flask(__name__)
bq_client = bigquery.Client(project=PROJECT_ID)

firestore_client = firestore.Client(project=PROJECT_ID)

# -------------------------
# BIGQUERY HELPERS
# -------------------------

def run_query(query: str, job_config=None):
    return list(bq_client.query(query, job_config=job_config).result())


def get_user_record(user_id: str):
    query = f"""
    SELECT
      user_id,
      chat_id,
      username,
      first_name,
      last_name,
      language_code,
      is_bot,
      chat_type,
      chat_title,
      alerts_enabled,
      snoozed_until,
      last_alert,
      last_command,
      alert_count,
      created_at,
      updated_at
    FROM `{USERS_TABLE}`
    WHERE user_id = @user_id
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
        ]
    )
    rows = run_query(query, job_config)
    return rows[0] if rows else None


def get_telegram_user_chat_data(update: Update):
    user = update.effective_user
    chat = update.effective_chat

    return {
        "user_id": str(user.id) if user else None,
        "chat_id": str(chat.id) if chat else None,
        "username": user.username if user else None,
        "first_name": user.first_name if user else None,
        "last_name": user.last_name if user else None,
        "language_code": user.language_code if user else None,
        "is_bot": user.is_bot if user else None,
        "chat_type": chat.type if chat else None,
        "chat_title": getattr(chat, "title", None) if chat else None,
    }


def upsert_user_record(
    user_id: str,
    chat_id: str,
    username: str | None,
    first_name: str | None,
    last_name: str | None,
    language_code: str | None,
    is_bot: bool | None,
    chat_type: str | None,
    chat_title: str | None
):
    query = f"""
    MERGE `{USERS_TABLE}` T
    USING (
      SELECT
        @user_id AS user_id,
        @chat_id AS chat_id,
        @username AS username,
        @first_name AS first_name,
        @last_name AS last_name,
        @language_code AS language_code,
        @is_bot AS is_bot,
        @chat_type AS chat_type,
        @chat_title AS chat_title,
        TRUE AS alerts_enabled,
        CAST(NULL AS TIMESTAMP) AS snoozed_until,
        CAST(NULL AS STRING) AS last_alert,
        CAST(NULL AS STRING) AS last_command,
        0 AS alert_count,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
    ) S
    ON T.user_id = S.user_id
    WHEN MATCHED THEN
      UPDATE SET
        chat_id = S.chat_id,
        username = S.username,
        first_name = S.first_name,
        last_name = S.last_name,
        language_code = S.language_code,
        is_bot = S.is_bot,
        chat_type = S.chat_type,
        chat_title = S.chat_title,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (
        user_id,
        chat_id,
        username,
        first_name,
        last_name,
        language_code,
        is_bot,
        chat_type,
        chat_title,
        alerts_enabled,
        snoozed_until,
        last_alert,
        last_command,
        alert_count,
        created_at,
        updated_at
      )
      VALUES (
        S.user_id,
        S.chat_id,
        S.username,
        S.first_name,
        S.last_name,
        S.language_code,
        S.is_bot,
        S.chat_type,
        S.chat_title,
        S.alerts_enabled,
        S.snoozed_until,
        S.last_alert,
        S.last_command,
        S.alert_count,
        S.created_at,
        S.updated_at
      )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("chat_id", "STRING", chat_id),
            bigquery.ScalarQueryParameter("username", "STRING", username),
            bigquery.ScalarQueryParameter("first_name", "STRING", first_name),
            bigquery.ScalarQueryParameter("last_name", "STRING", last_name),
            bigquery.ScalarQueryParameter("language_code", "STRING", language_code),
            bigquery.ScalarQueryParameter("is_bot", "BOOL", is_bot),
            bigquery.ScalarQueryParameter("chat_type", "STRING", chat_type),
            bigquery.ScalarQueryParameter("chat_title", "STRING", chat_title),
        ]
    )
    bq_client.query(query, job_config=job_config).result()


def set_alerts_enabled(user_id: str, enabled: bool):
    query = f"""
    UPDATE `{USERS_TABLE}`
    SET
      alerts_enabled = @enabled,
      updated_at = CURRENT_TIMESTAMP()
    WHERE user_id = @user_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("enabled", "BOOL", enabled),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        ]
    )
    bq_client.query(query, job_config=job_config).result()


def set_snooze(user_id: str, snoozed_until):
    query = f"""
    UPDATE `{USERS_TABLE}`
    SET
      alerts_enabled = TRUE,
      snoozed_until = @snoozed_until,
      updated_at = CURRENT_TIMESTAMP()
    WHERE user_id = @user_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snoozed_until", "TIMESTAMP", snoozed_until),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        ]
    )
    bq_client.query(query, job_config=job_config).result()


def clear_snooze(user_id: str):
    query = f"""
    UPDATE `{USERS_TABLE}`
    SET
      snoozed_until = NULL,
      updated_at = CURRENT_TIMESTAMP()
    WHERE user_id = @user_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        ]
    )
    bq_client.query(query, job_config=job_config).result()


def update_last_command(user_id: str, command: str):
    query = f"""
    UPDATE `{USERS_TABLE}`
    SET
      last_command = @command,
      updated_at = CURRENT_TIMESTAMP()
    WHERE user_id = @user_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("command", "STRING", command),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        ]
    )
    bq_client.query(query, job_config=job_config).result()


def build_today_message():
    query = """
    SELECT
      RaceTime,
      RaceLocation,
      HorseName,
      Odds
    FROM `horseracing-pacey32-github.bettingalerts.2_SelectedHorses`
    ORDER BY RaceTime, RaceLocation, HorseName
    """
    rows = run_query(query)

    if not rows:
        return "🐎 Today's Picks\n\nNo selections today."

    lines = ["🐎 Today's Picks", ""]
    for row in rows:
        lines.append(
            f"{row.RaceTime} {row.RaceLocation} - {row.HorseName} ({row.Odds})"
        )
    return "\n".join(lines)


def build_results_message():
    query = """
    SELECT *
    FROM `horseracing-pacey32-github.bettingalerts.3_YesterdaysResults`
    ORDER BY RaceLocation, RaceTime, HorseName
    """
    rows = run_query(query)

    if not rows:
        return "📊 Today's Results\n\nNo results found."

    lines = ["📊 Today's Results", ""]
    for row in rows:
        race_time = getattr(row, "RaceTime", "")
        course = getattr(row, "RaceLocation", "")
        horse = getattr(row, "HorseName", "")
        result = getattr(row, "Result", "")

        line = f"{race_time} {course} - {horse}"
        if result:
            line += f" | {result}"

        lines.append(line)

    return "\n".join(lines)


def prepare_user(update: Update, command_name: str):
    telegram_data = get_telegram_user_chat_data(update)

    upsert_user_record(
        user_id=telegram_data["user_id"],
        chat_id=telegram_data["chat_id"],
        username=telegram_data["username"],
        first_name=telegram_data["first_name"],
        last_name=telegram_data["last_name"],
        language_code=telegram_data["language_code"],
        is_bot=telegram_data["is_bot"],
        chat_type=telegram_data["chat_type"],
        chat_title=telegram_data["chat_title"],
    )

    update_last_command(telegram_data["user_id"], command_name)

    return telegram_data["user_id"], telegram_data["chat_id"]


def create_telegram_account_link(
    code: str,
    telegram_user_id: str,
    chat_id: str,
) -> tuple[bool, str]:
    """Redeem a linking code or request a confirmed transfer."""

    if not re.fullmatch(r"[0-9a-f]{64}", code):
        return False, "Invalid linking code."

    code_hash = hashlib.sha256(
        code.encode("utf-8")
    ).hexdigest()

    code_ref = firestore_client.collection(
        LINK_CODES_COLLECTION
    ).document(code_hash)

    firebase_links = firestore_client.collection(
        FIREBASE_LINKS_COLLECTION
    )

    telegram_links = firestore_client.collection(
        "telegramUserLinks"
    )

    pending_ref = firestore_client.collection(
        PENDING_TRANSFERS_COLLECTION
    ).document(telegram_user_id)

    transaction = firestore_client.transaction()

    @firestore.transactional
    def link(transaction):
        code_snapshot = code_ref.get(
            transaction=transaction
        )

        if not code_snapshot.exists:
            return (
                False,
                "Invalid or expired linking code.",
                False,
            )

        data = code_snapshot.to_dict() or {}
        now = datetime.now(timezone.utc)

        if data.get("usedAt") is not None:
            return (
                False,
                "This linking code has already been used.",
                False,
            )

        expires_at = data.get("expiresAt")

        if expires_at is None or expires_at <= now:
            return (
                False,
                "This linking code has expired.",
                False,
            )

        target_uid = data.get("firebaseUID")

        if not target_uid:
            return False, "Invalid linking code.", False

        target_ref = firebase_links.document(
            target_uid
        )

        telegram_ref = telegram_links.document(
            telegram_user_id
        )

        target_snapshot = target_ref.get(
            transaction=transaction
        )

        telegram_snapshot = telegram_ref.get(
            transaction=transaction
        )

        existing_link = (
            telegram_snapshot.to_dict()
            if telegram_snapshot.exists
            else {}
        )

        source_uid = existing_link.get("firebaseUID")

        source_ref = (
            firebase_links.document(source_uid)
            if source_uid
            else None
        )

        source_snapshot = (
            source_ref.get(transaction=transaction)
            if source_ref is not None
            else None
        )

        # All Firestore reads must precede writes.

        if target_snapshot.exists:
            return (
                False,
                "This Pacey32 account already has a Telegram connection.",
                False,
            )

        if telegram_snapshot.exists:
            if not source_uid or source_snapshot is None:
                return (
                    False,
                    "Existing Telegram connection is invalid.",
                    False,
                )

            if not source_snapshot.exists:
                return (
                    False,
                    "Existing Telegram connection needs recovery.",
                    False,
                )

            source_data = source_snapshot.to_dict() or {}

            if (
                str(source_data.get("telegramUserID"))
                != telegram_user_id
            ):
                return (
                    False,
                    "Existing account mapping is inconsistent.",
                    False,
                )

            # The Telegram account is linked to a different
            # Firebase account. Require explicit confirmation.
            transaction.set(
                pending_ref,
                {
                    "sourceFirebaseUID": source_uid,
                    "targetFirebaseUID": target_uid,
                    "telegramUserID": telegram_user_id,
                    "chatID": chat_id,
                    "createdAt": firestore.SERVER_TIMESTAMP,
                    "expiresAt": expires_at,
                },
            )

            transaction.update(
                code_ref,
                {
                    "usedAt": firestore.SERVER_TIMESTAMP
                },
            )

            return (
                True,
                "This Telegram account is linked to another "
                "Pacey32 account.\n\n"
                "To transfer it to the account you just "
                "signed into, send /confirmtransfer "
                "within 10 minutes.\n\n"
                "Your saved scenarios and betting history "
                "will stay with their existing accounts.",
                False,
            )

        # New account linking (no transfer required).
        transaction.create(
            target_ref,
            {
                "telegramUserID": telegram_user_id,
                "chatID": chat_id,
                "linkedAt": firestore.SERVER_TIMESTAMP,
            },
        )

        transaction.create(
            telegram_ref,
            {
                "firebaseUID": target_uid,
                "chatID": chat_id,
                "linkedAt": firestore.SERVER_TIMESTAMP,
            },
        )

        transaction.update(
            code_ref,
            {
                "usedAt": firestore.SERVER_TIMESTAMP
            },
        )

        return (
            True,
            "Your Telegram account has been linked successfully.",
            True,
        )

    success, message, completed = link(transaction)

    if completed:
        try:
            sync_telegram_account_links()
        except Exception:
            app.logger.exception(
                "Telegram linked, but BigQuery sync failed."
            )

    return success, message


def confirm_telegram_account_transfer(
    telegram_user_id: str,
    chat_id: str,
) -> tuple[bool, str]:
    """Transfer an existing link after Telegram confirmation."""

    firebase_links = firestore_client.collection(
        FIREBASE_LINKS_COLLECTION
    )

    telegram_ref = firestore_client.collection(
        "telegramUserLinks"
    ).document(telegram_user_id)

    pending_ref = firestore_client.collection(
        PENDING_TRANSFERS_COLLECTION
    ).document(telegram_user_id)

    transaction = firestore_client.transaction()

    @firestore.transactional
    def transfer(transaction):
        pending_snapshot = pending_ref.get(
            transaction=transaction
        )

        if not pending_snapshot.exists:
            return (
                False,
                "No pending transfer. Start from My Account "
                "on Pacey32.",
            )

        pending = pending_snapshot.to_dict() or {}

        now = datetime.now(timezone.utc)

        if (
            not pending.get("expiresAt")
            or pending["expiresAt"] <= now
        ):
            return (
                False,
                "Transfer request expired. Generate a new "
                "link from My Account.",
            )

        if (
            str(pending.get("telegramUserID"))
            != telegram_user_id
            or str(pending.get("chatID")) != chat_id
        ):
            return False, "Transfer verification failed."

        source_uid = pending.get("sourceFirebaseUID")
        target_uid = pending.get("targetFirebaseUID")

        if not source_uid or not target_uid:
            return False, "Invalid transfer request."

        source_ref = firebase_links.document(source_uid)
        target_ref = firebase_links.document(target_uid)

        source_snapshot = source_ref.get(
            transaction=transaction
        )

        target_snapshot = target_ref.get(
            transaction=transaction
        )

        telegram_snapshot = telegram_ref.get(
            transaction=transaction
        )

        # Validate both sides of the existing relationship.
        if (
            not source_snapshot.exists
            or not telegram_snapshot.exists
        ):
            return (
                False,
                "Existing connection has changed. "
                "Please start again.",
            )

        source_data = source_snapshot.to_dict() or {}
        telegram_data = telegram_snapshot.to_dict() or {}

        if (
            str(source_data.get("telegramUserID"))
            != telegram_user_id
            or telegram_data.get("firebaseUID") != source_uid
        ):
            return (
                False,
                "Existing connection has changed. "
                "Please start again.",
            )

        if target_snapshot.exists:
            return (
                False,
                "The destination Pacey32 account already "
                "has a Telegram connection.",
            )

        # Atomic transfer: old link removed, new link created.
        transaction.delete(source_ref)

        transaction.create(
            target_ref,
            {
                "telegramUserID": telegram_user_id,
                "chatID": chat_id,
                "linkedAt": firestore.SERVER_TIMESTAMP,
            },
        )

        transaction.update(
            telegram_ref,
            {
                "firebaseUID": target_uid,
                "chatID": chat_id,
                "linkedAt": firestore.SERVER_TIMESTAMP,
            },
        )

        transaction.delete(pending_ref)

        return (
            True,
            "Telegram connection transferred successfully. "
            "Return to My Account and select Check Connection.",
        )

    success, message = transfer(transaction)

    if success:
        try:
            sync_telegram_account_links()
        except Exception:
            app.logger.exception(
                "Telegram transfer succeeded, but "
                "BigQuery synchronisation failed."
            )

    return success, message




def sync_telegram_account_links():
    """
    Synchronise authoritative Firestore Telegram links
    into BigQuery, including deleted or transferred links.
    """

    firebase_links = firestore_client.collection(
        FIREBASE_LINKS_COLLECTION
    )

    records = []

    for document in firebase_links.stream():
        data = document.to_dict() or {}

        firebase_uid = document.id
        telegram_user_id = data.get("telegramUserID")
        chat_id = data.get("chatID")
        linked_at = data.get("linkedAt")

        if not telegram_user_id or not chat_id or not linked_at:
            continue

        records.append({
            "FirebaseUID": firebase_uid,
            "TelegramUserID": str(telegram_user_id),
            "ChatID": str(chat_id),
            "LinkedAt": linked_at.isoformat(),
        })

    # Firestore is the authoritative source.
    # If no links remain, clear the BigQuery mirror.
    if not records:
        query = f"DELETE FROM `{LINKS_TABLE}` WHERE TRUE"
        bq_client.query(query).result()
        return 0

    query = f"""
    MERGE `{LINKS_TABLE}` T
    USING (
      SELECT
        FirebaseUID,
        TelegramUserID,
        ChatID,
        TIMESTAMP(LinkedAt) AS LinkedAt
      FROM UNNEST(@records)
    ) S
    ON T.FirebaseUID = S.FirebaseUID

    WHEN MATCHED THEN
      UPDATE SET
        TelegramUserID = S.TelegramUserID,
        ChatID = S.ChatID,
        UpdatedAt = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        FirebaseUID,
        TelegramUserID,
        ChatID,
        LinkedAt,
        UpdatedAt
      )
      VALUES (
        S.FirebaseUID,
        S.TelegramUserID,
        S.ChatID,
        S.LinkedAt,
        CURRENT_TIMESTAMP()
      )

    WHEN NOT MATCHED BY SOURCE THEN
      DELETE
    """

    parameters = [
        bigquery.StructQueryParameter(
            None,
            bigquery.ScalarQueryParameter(
                "FirebaseUID",
                "STRING",
                record["FirebaseUID"]
            ),
            bigquery.ScalarQueryParameter(
                "TelegramUserID",
                "STRING",
                record["TelegramUserID"]
            ),
            bigquery.ScalarQueryParameter(
                "ChatID",
                "STRING",
                record["ChatID"]
            ),
            bigquery.ScalarQueryParameter(
                "LinkedAt",
                "STRING",
                record["LinkedAt"]
            ),
        )
        for record in records
    ]

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "records",
                "RECORD",
                parameters
            )
        ]
    )

    bq_client.query(
        query,
        job_config=job_config
    ).result()

    return len(records)



# -------------------------
# COMMANDS
# -------------------------


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        if update.effective_chat.type != "private":
            await update.message.reply_text(
                "Account linking is only available in private chats."
            )
            return

        if len(context.args) != 1:
            await update.message.reply_text("Invalid linking code.")
            return

        telegram_user_id = str(update.effective_user.id)
        chat_id = str(update.effective_chat.id)

        try:
            success, message = create_telegram_account_link(
                code=context.args[0],
                telegram_user_id=telegram_user_id,
                chat_id=chat_id,
            )
        except Exception:
            app.logger.exception("Telegram account linking failed")
            await update.message.reply_text(
                "Account linking failed. Please try again."
            )
            return

        await update.message.reply_text(message)
        return

    prepare_user(update, "/start")

    await update.message.reply_text(
        "You are subscribed to alerts.\n"
        "Use /help to see available commands."
    )


async def confirmtransfer(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if (
        not update.effective_chat
        or update.effective_chat.type != "private"
        or not update.effective_user
        or not update.message
    ):
        return

    telegram_user_id = str(update.effective_user.id)
    chat_id = str(update.effective_chat.id)

    try:
        success, message = confirm_telegram_account_transfer(
            telegram_user_id=telegram_user_id,
            chat_id=chat_id,
        )

        await update.message.reply_text(message)

    except Exception:
        app.logger.exception(
            "Telegram account transfer failed"
        )

        await update.message.reply_text(
            "Transfer failed. Please try again."
        )


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id, _ = prepare_user(update, "/stop")
    set_alerts_enabled(user_id, False)
    clear_snooze(user_id)

    await update.message.reply_text(
        "You have unsubscribed from alerts.\n"
        "Send /start to subscribe again."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    prepare_user(update, "/help")

    await update.message.reply_text(
        "/start - Subscribe to alerts\n"
        "/stop - Unsubscribe from alerts\n"
        "/help - Show available commands\n"
        "/status - Show your alert status\n"
        "/pause - Pause your alerts\n"
        "/resume - Resume your alerts\n"
        "/today - Show today's picks\n"
        "/results - Show latest results\n"
        "/lastalert - Show the last alert sent to you\n"
        "/settings - Show your current settings\n"
        "/snooze 60 - Snooze alerts for 60 minutes"
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id, _ = prepare_user(update, "/status")

    try:
        record = get_user_record(user_id)

        if not record:
            await update.message.reply_text("No user record found.")
            return

        snoozed_until = record.snoozed_until

        if not record.alerts_enabled:
            status_text = "paused"
        elif snoozed_until:
            now_utc = datetime.now(timezone.utc)

            if snoozed_until.tzinfo is None:
                snoozed_until = snoozed_until.replace(tzinfo=timezone.utc)

            if now_utc < snoozed_until:
                status_text = f"snoozed until {snoozed_until}"
            else:
                status_text = "active"
        else:
            status_text = "active"

        await update.message.reply_text(f"Your alerts are {status_text}.")

    except Exception as e:
        print(f"STATUS ERROR: {e}")
        await update.message.reply_text("Status check failed.")


async def pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id, _ = prepare_user(update, "/pause")
    set_alerts_enabled(user_id, False)
    await update.message.reply_text("Your alerts are paused.")


async def resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id, _ = prepare_user(update, "/resume")
    set_alerts_enabled(user_id, True)
    clear_snooze(user_id)
    await update.message.reply_text("Your alerts are resumed.")


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    prepare_user(update, "/today")
    await update.message.reply_text(build_today_message())


async def results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    prepare_user(update, "/results")
    await update.message.reply_text(build_results_message())


async def lastalert_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id, _ = prepare_user(update, "/lastalert")
    record = get_user_record(user_id)

    if not record or not record.last_alert:
        await update.message.reply_text("No alert has been sent to you yet.")
        return

    await update.message.reply_text(record.last_alert)


async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id, _ = prepare_user(update, "/settings")
    record = get_user_record(user_id)

    if not record:
        await update.message.reply_text("No user record found.")
        return

    await update.message.reply_text(
        f"alerts_enabled: {record.alerts_enabled}\n"
        f"snoozed_until: {record.snoozed_until}\n"
        f"last_alert: {record.last_alert}\n"
        f"last_command: {record.last_command}\n"
        f"alert_count: {record.alert_count}"
    )


async def snooze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id, _ = prepare_user(update, "/snooze")

    if not context.args:
        await update.message.reply_text("Usage: /snooze 60")
        return

    try:
        minutes = int(context.args[0])
        if minutes <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Please provide a positive whole number of minutes.")
        return

    snoozed_until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    set_snooze(user_id, snoozed_until)

    await update.message.reply_text(
        f"Your alerts are snoozed for {minutes} minutes.\n"
        f"Snoozed until: {snoozed_until}"
    )


# -------------------------
# TELEGRAM APP FACTORY
# -------------------------

def build_telegram_app():
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("confirmtransfer", confirmtransfer))
    application.add_handler(CommandHandler("stop", stop))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("pause", pause))
    application.add_handler(CommandHandler("resume", resume))
    application.add_handler(CommandHandler("today", today))
    application.add_handler(CommandHandler("results", results))
    application.add_handler(CommandHandler("lastalert", lastalert_command))
    application.add_handler(CommandHandler("settings", settings))
    application.add_handler(CommandHandler("snooze", snooze))

    return application


async def process_telegram_update(data: dict):
    application = build_telegram_app()
    await application.initialize()
    await application.start()

    try:
        update = Update.de_json(data, application.bot)
        await application.process_update(update)
    finally:
        await application.stop()
        await application.shutdown()


# -------------------------
# HEALTHCHECK
# -------------------------

@app.get("/")
def health():
    return "Bot running", 200


# -------------------------
# TELEGRAM WEBHOOK
# -------------------------

@app.post("/webhook")
def webhook():
    secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if secret != TELEGRAM_WEBHOOK_SECRET:
        abort(403)

    data = request.get_json(silent=True)
    if not data:
        abort(400)

    asyncio.run(process_telegram_update(data))

    return "ok", 200
