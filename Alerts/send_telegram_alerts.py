import os
import json
import requests
from pathlib import Path
from datetime import datetime, timezone
from google.cloud import bigquery

PROJECT_ID = "horseracing-pacey32-github"

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALERT_TYPE = os.environ["ALERT_TYPE"]  # morning or evening

STATE_FILE = Path(__file__).with_name("user_state.json")


def load_state():
    if not STATE_FILE.exists():
        return {}

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def parse_snoozed_until(value):
    if not value:
        return None
    return datetime.fromisoformat(value)


def user_should_receive_alert(record: dict) -> bool:
    if not record.get("alerts_enabled", True):
        return False

    snoozed_until = record.get("snoozed_until")
    if snoozed_until:
        snoozed_dt = parse_snoozed_until(snoozed_until)
        now = datetime.now(timezone.utc)

        if snoozed_dt and now < snoozed_dt:
            return False

    return True


def run_query(query: str):
    client = bigquery.Client(project=PROJECT_ID)
    return list(client.query(query).result())


def send_telegram_message(chat_id: str, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    response = requests.post(
        url,
        data={
            "chat_id": chat_id,
            "text": text
        },
        timeout=30
    )
    response.raise_for_status()


def build_morning_message():
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


def build_evening_message():
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


def main():
    if ALERT_TYPE == "morning":
        message = build_morning_message()
    elif ALERT_TYPE == "evening":
        message = build_evening_message()
    else:
        raise ValueError("ALERT_TYPE must be 'morning' or 'evening'")

    state = load_state()

    for user_id, record in state.items():
        if not user_should_receive_alert(record):
            continue

        chat_id = record.get("chat_id")
        if not chat_id:
            continue

        send_telegram_message(chat_id, message)
        record["last_alert"] = message

    save_state(state)


if __name__ == "__main__":
    main()