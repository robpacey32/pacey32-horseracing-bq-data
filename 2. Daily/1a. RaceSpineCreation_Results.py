# ===============================================================
# 🏇 SPORTING LIFE RACE SPINE CREATION — RESULTS (22:00)
# ---------------------------------------------------------------
# Pulls today's full race data (both URLs) from Sporting Life.
# Uses same logic as the historic version.
# Status:
#   - Pending (default)
#   - Abandoned (if detected)
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
# 🧩 FUNCTION: get_race_urls
# ===============================================================
def get_race_urls(results_date, debug=False):
    """
    Returns DataFrame:
    Date | Location | Time | prerace_URL | postrace_URL | Status
    """
    base_url = f"https://www.sportinglife.com/racing/results/{results_date}"

    try:
        response = requests.get(base_url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
    except Exception as e:
        print(f"[get_race_urls] Error fetching {base_url}: {e}")
        return pd.DataFrame(columns=["Date","Location","Time","prerace_URL","postrace_URL","Status"])

    soup = BeautifulSoup(response.content, "html.parser")
    meetings = soup.find_all("div", attrs={"data-testid":"meeting-summary"})
    if debug:
        print(f"[get_race_urls] Found {len(meetings)} meetings")

    url_rows = []

    for meeting in meetings:
        # --- Location ---
        course_elem = meeting.find("span", attrs={"data-test-id":"course-name"})
        location = course_elem.get_text(strip=True) if course_elem else "N/A"

        # --- Races in meeting ---
        race_containers = meeting.find_all("div", attrs={"data-test-id":"race-container"})
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

                # Build both URLs
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

                # --- Status logic ---
                status = "Pending"
                abandoned_elem = race.find("div", class_=re.compile(r"AbandonedIcon|Abandoned"))
                if abandoned_elem:
                    status = "Abandoned"
                    postrace_url = ""  # No results for abandoned races

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

    df = pd.DataFrame(url_rows, columns=["Date","Location","Time","prerace_URL","postrace_URL","Status"])

    if debug:
        print(f"[get_race_urls] Extracted {len(df)} races for {results_date}")

    return df


# ===============================================================
# ☁️ FUNCTION: Write to BigQuery
# ===============================================================
def write_spine_to_bq(df, project_id="horseracing-pacey32-github", dataset="horseracescrape",
                      table="RaceSpine", key_path="key.json"):
    """Append the DataFrame to BigQuery."""
    if df.empty:
        print("⚠️ No races found — skipping BigQuery upload.")
        return

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
    today_str = datetime.today().strftime("%Y-%m-%d")
    print(f"Fetching races for {today_str}...")
    today_df = get_race_urls(today_str, debug=True)
    write_spine_to_bq(today_df)
    print(f"🏁 Process completed — {len(today_df)} races uploaded.")