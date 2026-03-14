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

def get_user_record(user_id: str):
    query = f"""
    SELECT
      user_id,
      chat_id,
      alerts_enabled,
      snoozed_until,
      last_alert,
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
    rows = list(bq_client.query(query, job_config=job_config).result())
    return rows[0] if rows else None


def upsert_user_record(user_id: str, chat_id: str):
    query = f"""
    MERGE `{USERS_TABLE}` T
    USING (
      SELECT
        @user_id AS user_id,
        @chat_id AS chat_id,
        TRUE AS alerts_enabled,
        CAST(NULL AS TIMESTAMP) AS snoozed_until,
        CAST(NULL AS STRING) AS last_alert,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
    ) S
    ON T.user_id = S.user_id
    WHEN MATCHED THEN
      UPDATE SET
        chat_id = S.chat_id,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (user_id, chat_id, alerts_enabled, snoozed_until, last_alert, created_at, updated_at)
      VALUES (S.user_id, S.chat_id, S.alerts_enabled, S.snoozed_until, S.last_alert, S.created_at, S.updated_at)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("chat_id", "STRING", chat_id),
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


# -------------------------
# COMMANDS
# -------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    chat_id = str(update.effective_chat.id)

    upsert_user_record(user_id, chat_id)

    await update.message.reply_text(
        "You are subscribed to alerts.\nUse /help to see available commands."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start - Subscribe to alerts\n"
        "/help - Show available commands\n"
        "/status - Show your alert status\n"
        "/pause - Pause your alerts\n"
        "/resume - Resume your alerts\n"
        "/lastalert - Show the last alert sent to you\n"
        "/settings - Show your current settings\n"
        "/snooze - Snooze alerts for a period, e.g. /snooze 60"
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    chat_id = str(update.effective_chat.id)

    try:
        upsert_user_record(user_id, chat_id)
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
    user_id = str(update.effective_user.id)
    set_alerts_enabled(user_id, False)
    await update.message.reply_text("Your alerts are paused.")


async def resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    set_alerts_enabled(user_id, True)
    clear_snooze(user_id)
    await update.message.reply_text("Your alerts are resumed.")


async def lastalert_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    record = get_user_record(user_id)

    if not record or not record.last_alert:
        await update.message.reply_text("No alert has been sent to you yet.")
        return

    await update.message.reply_text(record.last_alert)


async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    record = get_user_record(user_id)

    if not record:
        await update.message.reply_text("No user record found.")
        return

    await update.message.reply_text(
        f"alerts_enabled: {record.alerts_enabled}\n"
        f"snoozed_until: {record.snoozed_until}\n"
        f"last_alert: {record.last_alert}"
    )


async def snooze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)

    if not context.args:
        await update.message.reply_text(
            "Usage: /snooze 60\nExample: /snooze 60"
        )
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
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("pause", pause))
    application.add_handler(CommandHandler("resume", resume))
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