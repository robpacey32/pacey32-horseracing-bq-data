import os
import asyncio
from datetime import datetime, timedelta, timezone

from flask import Flask, request, abort
from google.cloud import bigquery
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# -------------------------
# CONFIG
# -------------------------

PROJECT_ID = "horseracing-pacey32-github"
USERS_TABLE = f"{PROJECT_ID}.bettingalerts.TelegramUsers"

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_WEBHOOK_SECRET = os.environ["TELEGRAM_WEBHOOK_SECRET"]

app = Flask(__name__)
bq_client = bigquery.Client(project=PROJECT_ID)

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


# -------------------------
# COMMANDS
# -------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    prepare_user(update, "/start")

    await update.message.reply_text(
        "You are subscribed to alerts.\n"
        "Use /help to see available commands."
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
