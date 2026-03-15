import os
import requests
from pymongo import MongoClient
from google.cloud import bigquery

# -------------------------
# CONFIG
# -------------------------
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
MONGO_URI = os.environ["MONGO_URI"]
PROJECT_ID = os.environ["PROJECT_ID"]

# Change these if your MongoDB database / collection use different names
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "betting_tracker")
MONGO_COLLECTION_NAME = os.environ.get("MONGO_COLLECTION_NAME", "users")

BROADCAST_TABLE = "horseracing-pacey32-github.bettingalerts.telegram_broadcasts"

# -------------------------
# CLIENTS
# -------------------------
bq_client = bigquery.Client(project=PROJECT_ID)
mongo_client = MongoClient(MONGO_URI)
users_collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]

# -------------------------
# TELEGRAM
# -------------------------
def send_telegram_message(chat_id: int, text: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text
    }

    try:
        response = requests.post(url, json=payload, timeout=20)
        response.raise_for_status()
        result = response.json()
        return result.get("ok", False)
    except Exception as e:
        print(f"Failed for chat_id {chat_id}: {e}")
        return False


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


def set_broadcast_processing(broadcast_id: str):
    query = f"""
    UPDATE `{BROADCAST_TABLE}`
    SET status = 'PROCESSING'
    WHERE CAST(broadcast_id AS STRING) = @broadcast_id
      AND status = 'PENDING'
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("broadcast_id", "STRING", broadcast_id)
        ]
    )
    bq_client.query(query, job_config=job_config).result()


def set_broadcast_complete(broadcast_id: str, success_count: int, failure_count: int):
    query = f"""
    UPDATE `{BROADCAST_TABLE}`
    SET
      status = 'SENT',
      sent_at = CURRENT_TIMESTAMP(),
      success_count = @success_count,
      failure_count = @failure_count
    WHERE CAST(broadcast_id AS STRING) = @broadcast_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("broadcast_id", "STRING", broadcast_id),
            bigquery.ScalarQueryParameter("success_count", "INT64", success_count),
            bigquery.ScalarQueryParameter("failure_count", "INT64", failure_count),
        ]
    )
    bq_client.query(query, job_config=job_config).result()


def set_broadcast_failed(broadcast_id: str):
    query = f"""
    UPDATE `{BROADCAST_TABLE}`
    SET status = 'FAILED'
    WHERE CAST(broadcast_id AS STRING) = @broadcast_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("broadcast_id", "STRING", broadcast_id)
        ]
    )
    bq_client.query(query, job_config=job_config).result()


# -------------------------
# MONGODB HELPERS
# -------------------------
def get_active_users():
    users = users_collection.find(
        {"alerts_enabled": True},
        {"chat_id": 1, "_id": 0}
    )
    return [u["chat_id"] for u in users if "chat_id" in u]


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

    print(f"Processing broadcast_id={broadcast_id}")

    try:
        set_broadcast_processing(broadcast_id)

        chat_ids = get_active_users()
        print(f"Found {len(chat_ids)} active users")

        success_count = 0
        failure_count = 0

        for chat_id in chat_ids:
            ok = send_telegram_message(chat_id, message_text)
            if ok:
                success_count += 1
            else:
                failure_count += 1

        set_broadcast_complete(broadcast_id, success_count, failure_count)
        print(f"Broadcast complete. Success={success_count}, Failure={failure_count}")

    except Exception as e:
        print(f"Broadcast failed: {e}")
        set_broadcast_failed(broadcast_id)
        raise


if __name__ == "__main__":
    main()