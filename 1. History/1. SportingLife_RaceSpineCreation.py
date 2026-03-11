# ===============================================================
# 🏇 SPORTING LIFE RACE SPINE CREATION SCRIPT
# ---------------------------------------------------------------
# Builds a master table ("spine") of horse races and URLs
# from Sporting Life results pages and uploads to BigQuery.
#
# Logic:
# - Preserves existing Status where possible:
#     * Complete          -> keep Complete
#     * Abandoned...      -> keep existing abandoned status
#     * otherwise         -> Pending
# - Intended for backfilling missed dates without losing current status
#
# Recommended use:
# - Set start_date_str to the first missing date
# - Set end_date_str to the last missing date you want to fill
# - This script APPENDS rows, so avoid reloading dates you already have
#   unless you are happy with duplicate rows
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


# ===============================================================
# 🧩 FUNCTION: get_existing_status_map
# ===============================================================
def get_existing_status_map(
    project_id="horseracing-pacey32-github",
    dataset="horseracescrape",
    table="RaceSpine",
    key_path="key.json"
):
    """
    Returns a dict:
        prerace_URL -> best existing Status

    Priority:
        1. Complete
        2. Abandoned...
        3. Pending / anything else
    """
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=project_id)

    query = f"""
    SELECT prerace_URL, Status
    FROM `{project_id}.{dataset}.{table}`
    WHERE prerace_URL IS NOT NULL
    """

    df = client.query(query).to_dataframe()

    def rank_status(status):
        if status == "Complete":
            return 3
        if isinstance(status, str) and status.startswith("Abandoned"):
            return 2
        return 1

    status_map = {}

    for _, row in df.iterrows():
        url = row["prerace_URL"]
        status = row["Status"]

        if url not in status_map:
            status_map[url] = status
        else:
            if rank_status(status) > rank_status(status_map[url]):
                status_map[url] = status

    print(f"Loaded {len(status_map)} existing prerace_URL statuses from BigQuery")
    return status_map


# ===============================================================
# 🧩 FUNCTION: get_race_urls
# ===============================================================
def get_race_urls(results_date, existing_status_map=None, skip_prerace_urls=None, debug=False):
    """
    Returns DataFrame:
    Date | Location | Time | prerace_URL | postrace_URL | Status
    """
    base_url = f"https://www.sportinglife.com/racing/results/{results_date}"
    existing_status_map = existing_status_map or {}
    skip_prerace_urls = set(skip_prerace_urls or [])

    try:
        response = requests.get(
            base_url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        response.raise_for_status()
    except Exception as e:
        print(f"[get_race_urls] Error fetching {base_url}: {e}")
        return pd.DataFrame(columns=["Date", "Location", "Time", "prerace_URL", "postrace_URL", "Status"])

    soup = BeautifulSoup(response.content, "html.parser")
    meetings = soup.find_all("div", attrs={"data-testid": "meeting-summary"})

    if debug:
        print(f"[get_race_urls] Found {len(meetings)} meetings for {results_date}")

    url_rows = []

    for meeting in meetings:
        # --- Location ---
        course_elem = meeting.find("span", attrs={"data-testid": "course-name"})
        location = course_elem.get_text(strip=True) if course_elem else "N/A"

        # --- Races in meeting ---
        race_containers = meeting.find_all("div", attrs={"data-testid": "race-container"})
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

                if "/racecards/" in relative_url:
                    if "/racecard/" not in relative_url:
                        parts = relative_url.strip("/").split("/")
                        if len(parts) >= 6:
                            relative_url = f"/racing/racecards/{parts[2]}/{parts[3]}/racecard/{parts[4]}/{parts[5]}"

                    prerace_url = "https://www.sportinglife.com" + relative_url
                    postrace_url = "https://www.sportinglife.com" + relative_url.replace("/racecards/", "/results/")

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

                if prerace_url in skip_prerace_urls:
                    continue

                # --- Preserve existing status where appropriate ---
                existing_status = existing_status_map.get(prerace_url)

                if existing_status == "Complete":
                    status = "Complete"

                elif isinstance(existing_status, str) and existing_status.startswith("Abandoned"):
                    status = existing_status
                    postrace_url = ""

                else:
                    status = "Pending"

                    abandoned_elem = race.find("div", class_=re.compile(r"AbandonedIcon|Abandoned"))
                    if abandoned_elem:
                        status = "Abandoned"
                        postrace_url = ""

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

    df = pd.DataFrame(
        url_rows,
        columns=["Date", "Location", "Time", "prerace_URL", "postrace_URL", "Status"]
    )

    if debug:
        print(f"[get_race_urls] Extracted {len(df)} races for {results_date}")

    return df


# ===============================================================
# 🧩 FUNCTION: get_races_for_date_range
# ===============================================================
def get_races_for_date_range(start_date_str, end_date_str, existing_status_map=None, debug=False):
    """
    Fetches all races from Sporting Life for a date range.
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    dfs = []
    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")

        if debug:
            print(f"Fetching races for {date_str}...")

        df = get_race_urls(
            date_str,
            existing_status_map=existing_status_map,
            debug=debug
        )
        dfs.append(df)

        current_date += timedelta(days=1)

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
    else:
        combined_df = pd.DataFrame(columns=["Date", "Location", "Time", "prerace_URL", "postrace_URL", "Status"])

    return combined_df


# ===============================================================
# ☁️ FUNCTION: Write to BigQuery
# ===============================================================
def write_spine_to_bq(
    df,
    project_id="horseracing-pacey32-github",
    dataset="horseracescrape",
    table="RaceSpine",
    key_path="key.json"
):
    """
    Append the DataFrame to BigQuery using an explicit service account key file.
    """
    if df.empty:
        raise ValueError("No races found — scrape returned empty dataframe.")

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
    # -----------------------------------------------------------
    # Update these dates before running
    # -----------------------------------------------------------
    start_date_str = "2026-02-26"
    end_date_str = "2026-03-11"

    print(f"Backfilling RaceSpine from {start_date_str} to {end_date_str}")

    existing_status_map = get_existing_status_map()

    all_races_df = get_races_for_date_range(
        start_date_str,
        end_date_str,
        existing_status_map=existing_status_map,
        debug=True
    )

    if all_races_df.empty:
        raise RuntimeError(
            f"No races found for requested date range: {start_date_str} to {end_date_str}"
        )

    write_spine_to_bq(all_races_df)

    print(f"🏁 Process completed — {len(all_races_df)} races uploaded.")
