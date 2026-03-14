import os
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

STATE_FILE = Path(__file__).with_name("user_state.json")


def load_state():
    if not STATE_FILE.exists():
        return {}

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def get_user_record(update: Update, state: dict):
    user_id = str(update.effective_user.id)
    chat_id = str(update.effective_chat.id)

    if user_id not in state:
        state[user_id] = {
            "chat_id": chat_id,
            "alerts_enabled": True,
            "snoozed_until": None,
            "last_alert": None
        }
    else:
        # Keep chat_id fresh in case it ever changes
        state[user_id]["chat_id"] = chat_id

    return user_id, state[user_id]


def parse_snoozed_until(value):
    if not value:
        return None
    return datetime.fromisoformat(value)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    _, record = get_user_record(update, state)
    record["alerts_enabled"] = True
    save_state(state)

    await update.message.reply_text(
        "You are subscribed to alerts.\n"
        "Use /help to see available commands."
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
    state = load_state()
    _, record = get_user_record(update, state)
    save_state(state)

    alerts_enabled = record["alerts_enabled"]
    snoozed_until = record["snoozed_until"]

    if not alerts_enabled:
        status_text = "paused"
    elif snoozed_until:
        snoozed_dt = parse_snoozed_until(snoozed_until)
        now = datetime.now(timezone.utc)

        if snoozed_dt and now < snoozed_dt:
            status_text = f"snoozed until {snoozed_until}"
        else:
            status_text = "active"
    else:
        status_text = "active"

    await update.message.reply_text(f"Your alerts are {status_text}.")


async def pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    _, record = get_user_record(update, state)
    record["alerts_enabled"] = False
    save_state(state)

    await update.message.reply_text("Your alerts are paused.")


async def resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    _, record = get_user_record(update, state)
    record["alerts_enabled"] = True
    record["snoozed_until"] = None
    save_state(state)

    await update.message.reply_text("Your alerts are resumed.")


async def lastalert_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    _, record = get_user_record(update, state)
    save_state(state)

    last_alert = record["last_alert"] or "No alert has been sent to you yet."
    await update.message.reply_text(last_alert)


async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    _, record = get_user_record(update, state)
    save_state(state)

    await update.message.reply_text(
        f"alerts_enabled: {record['alerts_enabled']}\n"
        f"snoozed_until: {record['snoozed_until']}\n"
        f"last_alert: {record['last_alert']}"
    )


async def snooze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    _, record = get_user_record(update, state)

    if not context.args:
        await update.message.reply_text(
            "Usage: /snooze 60\n"
            "Example: /snooze 60 to snooze alerts for 60 minutes."
        )
        return

    try:
        minutes = int(context.args[0])
        if minutes <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "Please provide a positive whole number of minutes.\n"
            "Example: /snooze 60"
        )
        return

    snoozed_until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    record["alerts_enabled"] = True
    record["snoozed_until"] = snoozed_until.isoformat()
    save_state(state)

    await update.message.reply_text(
        f"Your alerts are snoozed for {minutes} minutes.\n"
        f"Snoozed until: {record['snoozed_until']}"
    )


def main():
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("pause", pause))
    app.add_handler(CommandHandler("resume", resume))
    app.add_handler(CommandHandler("lastalert", lastalert_command))
    app.add_handler(CommandHandler("settings", settings))
    app.add_handler(CommandHandler("snooze", snooze))

    app.run_polling()


if __name__ == "__main__":
    main()