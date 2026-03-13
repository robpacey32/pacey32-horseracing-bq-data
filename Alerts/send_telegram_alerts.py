import os
import requests
from google.cloud import bigquery

PROJECT_ID = "horseracing-pacey32-github"

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
ALERT_TYPE = os.environ["ALERT_TYPE"]  # morning or evening


def run_query(query: str):
    client = bigquery.Client(project=PROJECT_ID)
    return list(client.query(query).result())


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    response = requests.post(
        url,
        data={
            "chat_id": TELEGRAM_CHAT_ID,
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
    ORDER BY RaceTime, Course, HorseName
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

    send_telegram_message(message)


if __name__ == "__main__":
    main()
