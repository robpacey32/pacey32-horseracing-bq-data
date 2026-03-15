import os
import requests
from google.cloud import bigquery

# -------------------------
# CONFIG
# -------------------------
PROJECT_ID = "horseracing-pacey32-github"
BROADCAST_TABLE = "horseracing-pacey32-github.bettingalerts.telegram_broadcasts"
USERS_TABLE = "horseracing-pacey32-github.bettingalerts.TelegramUsers"

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]

# -------------------------
# CLIENT
# -------------------------
bq_client = bigquery.Client(project=PROJECT_ID)


# -------------------------
# BIGQUERY HELPERS
# -------------------------
def get_next_broadcast():
    query = f"""
    SELECT
      CAST(broadcast_id AS STRING) AS broadcast_id,
      message_text
    FROM `{BROADCAST_TABLE}`
    WHERE status = 'PENDING'
      AND scheduled_for <= CURRENT_TIMESTAMP()
    ORDER BY scheduled_for ASC, created_at ASC
    LIMIT 1
    """
    rows = list(bq_client.query(query).result())
    return rows[0] if rows else None


def set_broadcast_status(broadcast_id: str, status: str):
    query = f"""
    UPDATE `{BROADCAST_TABLE}`
    SET
      status = @status,
      sent_at = CASE
        WHEN @status = 'SENT' THEN CURRENT_TIMESTAMP()
        ELSE sent_at
      END
    WHERE CAST(broadcast_id AS STRING) = @broadcast_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("broadcast_id", "STRING", broadcast_id),
            bigquery.ScalarQueryParameter("status", "STRING", status),
        ]
    )
    bq_client.query(query, job_config=job_config).result()


def get_active_chat_ids():
    query = f"""
    SELECT DISTINCT CAST(chat_id AS STRING) AS chat_id
    FROM `{USERS_TABLE}`
    WHERE alerts_enabled = TRUE
    """
    rows = list(bq_client.query(query).result())
    return [row["chat_id"] for row in rows if row["chat_id"]]


# -------------------------
# TELEGRAM HELPERS
# -------------------------
def send_telegram_message(chat_id: str, text: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        return result.get("ok", False)
    except Exception as e:
        print(f"Failed to send to {chat_id}: {e}")
        return False


# -------------------------
# MAIN
# -------------------------
def main():
    broadcast = get_next_broadcast()

    if not broadcast:
        print("No pending broadcast found.")
        return

    broadcast_id = str(broadcast["broadcast_id"])
    message_text = broadcast["message_text"]

    print(f"Processing broadcast {broadcast_id}")

    try:
        set_broadcast_status(broadcast_id, "PROCESSING")

        chat_ids = get_active_chat_ids()
        print(f"Found {len(chat_ids)} active users")

        success_count = 0
        failure_count = 0

        for chat_id in chat_ids:
            ok = send_telegram_message(chat_id, message_text)
            if ok:
                success_count += 1
            else:
                failure_count += 1

        print(f"Broadcast complete. Success={success_count}, Failure={failure_count}")
        set_broadcast_status(broadcast_id, "SENT")

    except Exception as e:
        print(f"Broadcast failed: {e}")
        set_broadcast_status(broadcast_id, "FAILED")
        raise


if __name__ == "__main__":
    main()
