import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

alerts_paused = False
last_alert = "No alerts sent yet."


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Bot is running.")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start - Start the bot\n"
        "/help - Show commands\n"
        "/status - Show bot status\n"
        "/pause - Pause alerts\n"
        "/resume - Resume alerts\n"
        "/lastalert - Show last alert\n"
        "/settings - Show current settings"
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_text = "paused" if alerts_paused else "active"
    await update.message.reply_text(f"Bot status: {status_text}")


async def pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global alerts_paused
    alerts_paused = True
    await update.message.reply_text("Alerts paused.")


async def resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global alerts_paused
    alerts_paused = False
    await update.message.reply_text("Alerts resumed.")


async def lastalert_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(last_alert)


async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_text = "paused" if alerts_paused else "active"
    await update.message.reply_text(f"alerts_paused: {status_text}")


def main():
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("pause", pause))
    app.add_handler(CommandHandler("resume", resume))
    app.add_handler(CommandHandler("lastalert", lastalert_command))
    app.add_handler(CommandHandler("settings", settings))

    app.run_polling()


if __name__ == "__main__":
    main()
