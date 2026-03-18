import os
import requests
from google.cloud import bigquery

PROJECT_ID = "horseracing-pacey32-github"
USERS_TABLE = f"{PROJECT_ID}.bettingalerts.TelegramUsers"

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALERT_TYPE = os.environ["ALERT_TYPE"]  # morning or evening


def run_query(query: str, job_config=None):
    client = bigquery.Client(project=PROJECT_ID)
    return list(client.query(query, job_config=job_config).result())


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


def get_active_users():
    query = f"""
    SELECT
      user_id,
      chat_id
    FROM `{USERS_TABLE}`
    WHERE alerts_enabled = TRUE
      AND (snoozed_until IS NULL OR snoozed_until < CURRENT_TIMESTAMP())
    """
    return run_query(query)


def update_last_alert_and_count(user_id: str, message: str):
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
    UPDATE `{USERS_TABLE}`
    SET
      last_alert = @message,
      alert_count = COALESCE(alert_count, 0) + 1,
      updated_at = CURRENT_TIMESTAMP()
    WHERE user_id = @user_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("message", "STRING", message),
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
        ]
    )
    client.query(query, job_config=job_config).result()

def save_morning_selections():
    client = bigquery.Client(project=PROJECT_ID)

    query = """
    DELETE FROM `horseracing-pacey32-github.bettingalerts.3_DailySelectionsSent`
    WHERE AlertDate = CURRENT_DATE('Europe/London');

    INSERT INTO `horseracing-pacey32-github.bettingalerts.3_DailySelectionsSent` (
      AlertDate,
      RaceDateDt,
      RaceTime,
      RaceLocation,
      HorseName,
      Odds,
      SentAt
    )
    SELECT
      CURRENT_DATE('Europe/London') AS AlertDate,
      CAST(RaceDateDt AS DATE) AS RaceDateDt,
      RaceTime,
      RaceLocation,
      HorseName,
      Odds,
      CURRENT_TIMESTAMP() AS SentAt
    FROM `horseracing-pacey32-github.bettingalerts.2_SelectedHorses`;
    """
    client.query(query).result()
    

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
        save_morning_selections()
        message = build_morning_message()
    elif ALERT_TYPE == "evening":
        message = build_evening_message()
    else:
        raise ValueError("ALERT_TYPE must be 'morning' or 'evening'")

    users = get_active_users()

    for user in users:
        try:
            send_telegram_message(user.chat_id, message)
            update_last_alert_and_count(user.user_id, message)
            print(f"Sent {ALERT_TYPE} alert to user_id={user.user_id}")
        except Exception as e:
            print(f"Failed to send to user_id={user.user_id}, chat_id={user.chat_id}: {e}")


if __name__ == "__main__":
    main()
