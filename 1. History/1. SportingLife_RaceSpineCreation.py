# ===============================================================
# 🏇 SPORTING LIFE RACE SPINE CREATION SCRIPT
# ---------------------------------------------------------------
# Builds a master table ("spine") of all horse races and URLs
# from Sporting Life results pages and uploads to BigQuery.
# ===============================================================

# ------------------------------
# 📦 Imports
# ------------------------------
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

def get_race_urls(results_date, existing_urls=None, skip_prerace_urls=None, debug=False):
    """
    Returns DataFrame:
    Date | Location | Time | prerace_URL | postrace_URL | Status
    """
    base_url = f"https://www.sportinglife.com/racing/results/{results_date}"
    existing_urls = set(existing_urls or [])
    skip_prerace_urls = set(skip_prerace_urls or [])

    try:
        response = requests.get(base_url, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"[get_race_urls] Error fetching {base_url}: {e}")
        return pd.DataFrame(columns=["Date", "Location", "Time", "prerace_URL", "postrace_URL", "Status"])

    soup = BeautifulSoup(response.content, "html.parser")
    meetings = soup.find_all("div", attrs={"data-testid": "meeting-summary"})
    if debug:
        print(f"[get_race_urls] Found {len(meetings)} meetings")

    url_rows = []

    for meeting in meetings:
        # --- Location ---
        course_elem = meeting.find("span", attrs={"data-test-id": "course-name"})
        location = course_elem.get_text(strip=True) if course_elem else "N/A"

        # --- Races in meeting ---
        race_containers = meeting.find_all("div", attrs={"data-test-id": "race-container"})
        if debug:
            print(f"[get_race_urls] {location}: {len(race_containers)} races found")

        for race in race_containers:
            try:
                # --- Time ---
                time_elem = race.find("span", class_=re.compile(r"Race__RaceTime"))
                race_time = "N/A"
                if time_elem:
                    time_short = time_elem.find("span", class_=re.compile(r"time-short"))
                    race_time = time_short.get_text(strip=True) if time_short else "N/A"

                # --- Hyperlink and URLs ---
                a_tag = race.find("a", href=True)
                if not a_tag:
                    continue
                relative_url = a_tag["href"]

                # 🧩 Ensure correct structure for prerace URL (must include /racecard/)
                if "/racecards/" in relative_url:
                    if "/racecard/" not in relative_url:
                        # Expected format: /racing/racecards/YYYY-MM-DD/COURSE/RACEID/NAME
                        parts = relative_url.strip("/").split("/")
                        if len(parts) >= 6:
                            relative_url = f"/racing/racecards/{parts[2]}/{parts[3]}/racecard/{parts[4]}/{parts[5]}"
                    prerace_url = "https://www.sportinglife.com" + relative_url

                    # Build postrace URL (if it exists)
                    postrace_url = relative_url.replace("/racecards/", "/results/")
                    postrace_url = "https://www.sportinglife.com" + postrace_url

                elif "/results/" in relative_url:
                    postrace_url = "https://www.sportinglife.com" + relative_url
                    prerace_url = relative_url.replace("/results/", "/racecards/")
                    if "/racecard/" not in prerace_url:
                        parts = prerace_url.strip("/").split("/")
                        if len(parts) >= 6:
                            prerace_url = f"/racing/racecards/{parts[2]}/{parts[3]}/racecard/{parts[4]}/{parts[5]}"
                    prerace_url = "https://www.sportinglife.com" + prerace_url
                else:
                    continue

                # Skip unwanted URLs
                if prerace_url in skip_prerace_urls:
                    continue

                # --- Status (including abandoned reason if present) ---
                status = "Pending"
                abandoned_elem = race.find("div", class_=re.compile(r"AbandonedIcon|Abandoned"))
                if abandoned_elem:
                    status = "Abandoned"
                    meeting_header = race.find_parent(
                        "div",
                        class_="MeetingSummary__HorseRacingMeetingSummaryContainer-sc-a78c28b2-0"
                    )
                    if meeting_header:
                        reason_elem = meeting_header.find(
                            "span",
                            class_=re.compile(r"HeaderDetails__Abandoned")
                        )
                        if reason_elem:
                            full_reason = reason_elem.get_text(strip=True)
                            if full_reason:
                                status = full_reason  # e.g. "Abandoned: high winds"
                    postrace_url = ""  # Abandoned races have no results
                else:
                    # Not abandoned
                    if prerace_url in existing_urls:
                        status = "Complete"

                # Append row
                url_rows.append({
                    "Date": results_date,
                    "Location": location,
                    "Time": race_time,
                    "prerace_URL": prerace_url,
                    "postrace_URL": postrace_url,
                    "Status": status
                })

            except Exception as e:
                if debug:
                    print(f"[get_race_urls] Error parsing race: {e}")
                continue

    df = pd.DataFrame(url_rows, columns=["Date", "Location", "Time", "prerace_URL", "postrace_URL", "Status"])

    if debug:
        print(f"[get_race_urls] Extracted {len(df)} races for {results_date}")

    return df

# ===============================================================
# 🧩 FUNCTION: get_races_for_date_range
# ===============================================================
def get_races_for_date_range(start_date_str, end_date_str, debug=False):
    """
    Fetches all races from SportingLife for a range of dates.

    Args:
        start_date_str (str): Start date in "YYYY-MM-DD" format.
        end_date_str (str): End date in "YYYY-MM-DD" format.
        debug (bool): If True, prints debug info for each date.

    Returns:
        pd.DataFrame: Combined DataFrame of all races in the range.
    """
    # Convert strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    dfs = []
    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        if debug:
            print(f"Fetching races for {date_str}...")

        # Call your existing get_race_urls function
        df = get_race_urls(date_str, debug=debug)
        dfs.append(df)

        current_date += timedelta(days=1)

    # Combine all daily DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df

# ===============================================================
# ☁️ FUNCTION: Write to BigQuery
# ===============================================================

def write_spine_to_bq(df, project_id="horseracing-pacey32-github", dataset="horseracescrape", table="RaceSpine", key_path="key.json"):
    """Append the DataFrame to BigQuery using an explicit service account key file."""
    if df.empty:
        print("⚠️ No races found — skipping BigQuery upload.")
        return

    # --- Add a load timestamp ---
    df["load_timestamp"] = datetime.utcnow()
    table_id = f"{project_id}.{dataset}.{table}"

    # --- Authenticate using explicit key file ---
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=project_id)

    # --- Load to BigQuery ---
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
    start_date_str = "2025-11-11"
    end_date_str = "2025-11-11"

    all_races_df = get_races_for_date_range(start_date_str, end_date_str, debug=True)
    write_spine_to_bq(all_races_df)
    print(f"🏁 Process completed — {len(all_races_df)} races uploaded.")
