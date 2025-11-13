# ===============================================================
# 🏇 SPORTING LIFE RACE SPINE CREATION — PRERACE URLs (08:00)
# ---------------------------------------------------------------
# Pulls today's racecard URLs from Sporting Life.
# Builds a DataFrame:
#   Date | Location | Time | prerace_URL | postrace_URL | Status
# ===============================================================

# ------------------------------
# 📦 Imports
# ------------------------------
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account


# ===============================================================
# 🧩 FUNCTION: get_todays_races
# ===============================================================
def get_todays_races(debug=False):
    """
    Scrape today's races from Sporting Life racecards page.
    Returns DataFrame with prerace_URLs (postrace_URL blank).
    """
    today_str = datetime.today().strftime("%Y-%m-%d")
    base_url = f"https://www.sportinglife.com/racing/results/{today_str}"

    try:
        response = requests.get(base_url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
    except Exception as e:
        print(f"[get_todays_races] Error fetching {base_url}: {e}")
        return pd.DataFrame(columns=["Date","Location","Time","prerace_URL","postrace_URL","Status"])

    soup = BeautifulSoup(response.content, "html.parser")
    meetings = soup.find_all("div", attrs={"data-testid": "meeting-summary"})
    if debug:
        print(f"[get_todays_races] Found {len(meetings)} meetings for {today_str}")

    url_rows = []

    for meeting in meetings:
        # --- Location ---
        course_elem = meeting.find("span", attrs={"data-test-id": "course-name"})
        location = course_elem.get_text(strip=True) if course_elem else "N/A"

        # --- Meeting-level abandoned reason ---
        meeting_abandoned_reason = None
        header_reason = meeting.find("span", class_=re.compile(r"HeaderDetails__Abandoned"))
        if header_reason:
            meeting_abandoned_reason = header_reason.get_text(strip=True)

        # --- Races in meeting ---
        race_containers = meeting.find_all("div", attrs={"data-test-id": "race-container"})
        if debug:
            print(f"[get_todays_races] {location}: {len(race_containers)} races found")

        for race in race_containers:
            try:
                # --- Time ---
                time_elem = race.find("span", class_=re.compile(r"Race__RaceTime"))
                race_time = "N/A"
                if time_elem:
                    time_short = time_elem.find("span", class_=re.compile(r"time-short"))
                    race_time = time_short.get_text(strip=True) if time_short else "N/A"

                # --- Hyperlink (build prerace URL) ---
                a_tag = race.find("a", href=True)
                if not a_tag:
                    continue
                relative_url = a_tag["href"]

                # Convert URL properly
                if "/racecards/" in relative_url:
                    prerace_url = "https://www.sportinglife.com" + relative_url

                elif "/results/" in relative_url:
                    # convert to racecard
                    prerace_url = "https://www.sportinglife.com" + relative_url.replace("/results/", "/racecards/")

                else:
                    continue

                # --- Status logic ---
                if meeting_abandoned_reason:
                    status = meeting_abandoned_reason
                else:
                    status = "Pending"

                # Append row
                url_rows.append({
                    "Date": today_str,
                    "Location": location,
                    "Time": race_time,
                    "prerace_URL": prerace_url,
                    "postrace_URL": "",
                    "Status": status
                })

            except Exception as e:
                if debug:
                    print(f"[get_todays_races] Error parsing race: {e}")
                continue

    df = pd.DataFrame(url_rows, columns=["Date","Location","Time","prerace_URL","postrace_URL","Status"])

    if debug:
        print(f"[get_todays_races] Extracted {len(df)} races for {today_str}")

    return df


# ===============================================================
# ☁️ FUNCTION: Write to BigQuery
# ===============================================================
def write_spine_to_bq(df, project_id="horseracing-pacey32-github", dataset="horseracescrape",
                      table="RaceSpine", key_path="key.json"):
    """Append the DataFrame to BigQuery using an explicit service account key file."""
    if df.empty:
        print("⚠️ No races found — skipping BigQuery upload.")
        return

    # --- Add load timestamp ---
    df["load_timestamp"] = datetime.utcnow()
    table_id = f"{project_id}.{dataset}.{table}"

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=project_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"✅ Uploaded {len(df)} rows to {table_id}")


# ===============================================================
# ☁️ FUNCTION: Write to BigQuery
# ===============================================================
def write_spine_to_bq(df, project_id="horseracing-pacey32-github", dataset="horseracescrape",
                      table="RaceSpine", key_path="key.json"):
    """Append the DataFrame to BigQuery using an explicit service account key file."""
    if df.empty:
        print("⚠️ No races found — skipping BigQuery upload.")
        return

    # --- Add load timestamp ---
    df["load_timestamp"] = datetime.utcnow()
    table_id = f"{project_id}.{dataset}.{table}"

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=project_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"✅ Uploaded {len(df)} rows to {table_id}")


# ===============================================================
# 🚀 MAIN EXECUTION
# ===============================================================
if __name__ == "__main__":
    all_races_df = get_todays_races(debug=True)
    write_spine_to_bq(all_races_df)
    print(f"🏁 Process completed — {len(all_races_df)} races uploaded.")
