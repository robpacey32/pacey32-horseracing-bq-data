# ===============================================================
# 🏇 SPORTING LIFE RACE SPINE REPAIR — MISSING POSTRACE URLs
# ---------------------------------------------------------------
# Finds rows in RaceSpine_Latest where postrace_URL = ''
# Re-scrapes the Sporting Life results pages for those dates
# and appends repaired rows back into RaceSpine.
#
# Because RaceSpine_Latest keeps the latest row by:
#   PARTITION BY Date, Location, Time
#   ORDER BY load_timestamp DESC
#
# appending repaired rows is enough for the latest view to pick
# up the completed postrace_URL values.
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
# ⚙️ CONFIG
# ===============================================================
PROJECT_ID = "horseracing-pacey32-github"
DATASET = "horseracescrape"
SPINE_TABLE = "RaceSpine"
LATEST_VIEW = "RaceSpine_Latest"
KEY_PATH = "key.json"


# ===============================================================
# 🧩 FUNCTION: get_bigquery_client
# ===============================================================
def get_bigquery_client(
    project_id=PROJECT_ID,
    key_path=KEY_PATH
):
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return bigquery.Client(credentials=credentials, project=project_id)


# ===============================================================
# 🧩 FUNCTION: get_existing_status_map
# ===============================================================
def get_existing_status_map(
    project_id=PROJECT_ID,
    dataset=DATASET,
    table=SPINE_TABLE,
    key_path=KEY_PATH
):
    """
    Returns:
        dict of prerace_URL -> best existing Status

    Priority:
        1. Complete
        2. Abandoned...
        3. Pending / anything else
    """
    client = get_bigquery_client(project_id=project_id, key_path=key_path)

    query = f"""
    SELECT prerace_URL, Status
    FROM `{project_id}.{dataset}.{table}`
    WHERE prerace_URL IS NOT NULL
    """

    df = client.query(query).to_dataframe(create_bqstorage_client=False)

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
# 🧩 FUNCTION: get_missing_postrace_dates
# ===============================================================
def get_missing_postrace_dates(
    project_id=PROJECT_ID,
    dataset=DATASET,
    latest_view=LATEST_VIEW,
    key_path=KEY_PATH
):
    """
    Returns a sorted list of dates from RaceSpine_Latest where postrace_URL = ''
    """
    client = get_bigquery_client(project_id=project_id, key_path=key_path)

    query = f"""
    SELECT DISTINCT Date
    FROM `{project_id}.{dataset}.{latest_view}`
    WHERE postrace_URL = ''
    ORDER BY Date
    """

    df = client.query(query).to_dataframe(create_bqstorage_client=False)

    if df.empty:
        return []

    dates = [str(x) for x in df["Date"].tolist()]
    print(f"Found {len(dates)} dates with missing postrace_URL values")
    return dates


# ===============================================================
# 🧩 FUNCTION: get_race_urls
# ===============================================================
def get_race_urls(results_date, existing_status_map=None, debug=False):
    """
    Returns DataFrame:
    Date | Location | Time | prerace_URL | postrace_URL | Status
    """
    base_url = f"https://www.sportinglife.com/racing/results/{results_date}"
    existing_status_map = existing_status_map or {}

    try:
        response = requests.get(
            base_url,
            timeout=20,
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
# ☁️ FUNCTION: Write to BigQuery
# ===============================================================
def write_spine_to_bq(
    df,
    project_id=PROJECT_ID,
    dataset=DATASET,
    table=SPINE_TABLE,
    key_path=KEY_PATH
):
    """
    Append repaired rows to RaceSpine.
    """
    if df.empty:
        print("⚠️ No repaired races found — skipping BigQuery upload.")
        return

    df["load_timestamp"] = datetime.utcnow()
    table_id = f"{project_id}.{dataset}.{table}"

    client = get_bigquery_client(project_id=project_id, key_path=key_path)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"✅ Uploaded {len(df)} repaired rows to {table_id}")


# ===============================================================
# 🧩 FUNCTION: repair_missing_postrace_urls
# ===============================================================
def repair_missing_postrace_urls(debug=False):
    """
    1. Gets dates where RaceSpine_Latest has blank postrace_URL
    2. Re-scrapes those dates from Sporting Life results pages
    3. Appends repaired rows back into RaceSpine
    """
    missing_dates = get_missing_postrace_dates()
    if not missing_dates:
        print("✅ No missing postrace_URL dates found.")
        return

    existing_status_map = get_existing_status_map()

    repaired_dfs = []

    for i, missing_date in enumerate(missing_dates, start=1):
        print(f"[{i}/{len(missing_dates)}] Repairing {missing_date}...")
        repaired_df = get_race_urls(missing_date, existing_status_map=existing_status_map, debug=debug)

        if repaired_df.empty:
            print(f"⚠️ No rows found for {missing_date}")
            continue

        repaired_dfs.append(repaired_df)

    if not repaired_dfs:
        print("⚠️ No repaired dataframes produced.")
        return

    combined_df = pd.concat(repaired_dfs, ignore_index=True)
    write_spine_to_bq(combined_df)

    print(f"🏁 Repair completed — {len(combined_df)} rows appended.")


# ===============================================================
# 🚀 MAIN EXECUTION
# ===============================================================
if __name__ == "__main__":
    repair_missing_postrace_urls(debug=True)
