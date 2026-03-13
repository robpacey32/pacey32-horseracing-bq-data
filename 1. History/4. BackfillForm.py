# ===============================================================
# 🏇 SPORTING LIFE FORM BACKFILL
# ---------------------------------------------------------------
# Pulls historic prerace URLs from RaceSpine, scrapes only horse
# form data, and uploads to Scrape_PreRace_FormBackfill.
#
# Designed to run in GitHub Actions.
# ===============================================================

import os
import re
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from google.cloud import bigquery
from google.oauth2 import service_account

# ===============================================================
# ⚙️ CONFIGURATION
# ===============================================================
PROJECT_ID = "horseracing-pacey32-github"
DATASET_ID = "horseracescrape"
RACESPINE_TABLE = "RaceSpine"
BACKFILL_TABLE = "Scrape_PreRace_FormBackfill"
KEY_PATH = "key.json"

MAX_RACES = int(os.getenv("MAX_RACES", "10000000"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "2.5"))

# Optional lower date bound for testing, format YYYY-MM-DD
START_DATE = os.getenv("START_DATE", "")


# ===============================================================
# 🔗 BIGQUERY CLIENT
# ===============================================================
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


# ===============================================================
# 🧩 SELENIUM SETUP
# ===============================================================
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


# ===============================================================
# 🧰 SELENIUM HELPERS
# ===============================================================
def first_text(driver_or_elem, selectors, default="N/A"):
    for selector in selectors:
        try:
            elem = driver_or_elem.find_element(By.CSS_SELECTOR, selector)
            text = elem.text.strip()
            if text:
                return text
        except Exception:
            continue
    return default


def all_elements(driver_or_elem, selectors):
    for selector in selectors:
        try:
            elems = driver_or_elem.find_elements(By.CSS_SELECTOR, selector)
            if elems:
                return elems
        except Exception:
            continue
    return []


# ===============================================================
# 📥 LOAD HISTORIC URLS TO BACKFILL
# ===============================================================
def load_urls_to_backfill():
    client = get_bq_client()

    date_filter = ""
    if START_DATE:
        date_filter = f"AND SAFE_CAST(Date AS DATE) >= DATE('{START_DATE}')"

    query = f"""
        WITH spine_urls AS (
          SELECT DISTINCT
            CAST(prerace_URL AS STRING) AS prerace_URL
          FROM `{PROJECT_ID}.{DATASET_ID}.{RACESPINE_TABLE}`
          WHERE prerace_URL IS NOT NULL
            AND TRIM(CAST(prerace_URL AS STRING)) != ''
            {date_filter}
        ),
        already_done AS (
          SELECT DISTINCT
            CAST(SourceURL AS STRING) AS SourceURL
          FROM `{PROJECT_ID}.{DATASET_ID}.{BACKFILL_TABLE}`
          WHERE SourceURL IS NOT NULL
        )
        SELECT
          s.prerace_URL
        FROM spine_urls s
        LEFT JOIN already_done d
          ON s.prerace_URL = d.SourceURL
        WHERE d.SourceURL IS NULL
        LIMIT {MAX_RACES}
    """

    df = client.query(query).to_dataframe(create_bqstorage_client=False)

    if df.empty:
        print("⚠️ No URLs to backfill.")
    else:
        print(f"✅ Loaded {len(df)} URLs to backfill.")

    return df


# ===============================================================
# 🏗️ ENSURE BACKFILL TABLE EXISTS
# ===============================================================
def ensure_backfill_table():
    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{BACKFILL_TABLE}"

    schema = [
        bigquery.SchemaField("RaceDate", "STRING"),
        bigquery.SchemaField("RaceTime", "STRING"),
        bigquery.SchemaField("RaceLocation", "STRING"),
        bigquery.SchemaField("HorseName", "STRING"),
        bigquery.SchemaField("Form", "STRING"),
        bigquery.SchemaField("SourceURL", "STRING"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        client.get_table(table_id)
        print(f"✅ Table exists: {table_id}")
    except Exception:
        client.create_table(table)
        print(f"✅ Created table: {table_id}")


# ===============================================================
# 🐎 SCRAPE FORM ONLY
# ===============================================================
def scrape_prerace_form_only(driver, prerace_url, max_retries=3):
    data = []

    for attempt in range(max_retries):
        try:
            driver.get(prerace_url)
            time.sleep(SLEEP_SECONDS)

            race_date_txt = first_text(driver, [
                "p[class*='CourseListingHeader__StyledMainSubTitle']"
            ])

            try:
                date_obj = datetime.strptime(race_date_txt, "%A %d %B %Y")
                race_date = date_obj.strftime("%d/%m/%Y")
            except Exception:
                race_date = "N/A"

            race_location_txt = first_text(driver, [
                "p[class*='CourseListingHeader__StyledMainTitle']"
            ])

            if " " in race_location_txt:
                race_time, race_location = race_location_txt.split(" ", 1)
            else:
                race_time, race_location = "N/A", race_location_txt

            horse_containers = all_elements(driver, [
                "div[class*='Runner__StyledRunnerContainer']",
                "div[class*='RunnerContainer']"
            ])

            print(f"   Found {len(horse_containers)} horse containers")

            for parent in horse_containers:
                try:
                    horse_name = first_text(parent, [
                        "a[data-testid='horse-name-link']",
                        "a[data-test-id='horse-name-link']",
                        "a[href*='/horse/']"
                    ])

                    form_text = first_text(parent, [
                        "div[data-testid='show-form']",
                        "div[data-test-id='show-form']"
                    ], default="N/A")

                    form_match = re.search(r"Form:\s*(.+)$", form_text, re.I)
                    form = form_match.group(1).strip() if form_match else "N/A"

                    if horse_name != "N/A":
                        data.append({
                            "RaceDate": race_date,
                            "RaceTime": race_time,
                            "RaceLocation": race_location,
                            "HorseName": horse_name,
                            "Form": form,
                            "SourceURL": prerace_url
                        })

                except Exception as e:
                    print(f"   Horse parse error: {e}")
                    continue

            break

        except TimeoutException:
            print(f"   Timeout loading page ({attempt + 1}/{max_retries}): {prerace_url}")
            time.sleep(3)
        except Exception as e:
            print(f"   Page scrape error ({attempt + 1}/{max_retries}): {prerace_url} | {e}")
            time.sleep(3)

    return pd.DataFrame(data)


# ===============================================================
# ☁️ UPLOAD BATCH TO BIGQUERY
# ===============================================================
def upload_batch(df):
    if df.empty:
        print("⚠️ Empty batch — skipping upload.")
        return

    df = df.copy()
    df["load_timestamp"] = datetime.utcnow()

    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{BACKFILL_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("RaceDate", "STRING"),
            bigquery.SchemaField("RaceTime", "STRING"),
            bigquery.SchemaField("RaceLocation", "STRING"),
            bigquery.SchemaField("HorseName", "STRING"),
            bigquery.SchemaField("Form", "STRING"),
            bigquery.SchemaField("SourceURL", "STRING"),
            bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        ]
    )

    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print(f"✅ Uploaded {len(df)} rows to {table_id}")


# ===============================================================
# 🚀 MAIN
# ===============================================================
def main():
    ensure_backfill_table()

    df_urls = load_urls_to_backfill()
    if df_urls.empty:
        return

    driver = setup_driver()

    batch_frames = []
    failed_urls = []
    processed = 0

    try:
        for i, row in df_urls.iterrows():
            prerace_url = row["prerace_URL"]
            print(f"[{i+1}/{len(df_urls)}] Scraping: {prerace_url}")

            scraped_df = scrape_prerace_form_only(driver, prerace_url)

            if scraped_df.empty:
                print("   ⚠️ No data returned")
                failed_urls.append(prerace_url)
            else:
                batch_frames.append(scraped_df)
                processed += 1

            if len(batch_frames) >= BATCH_SIZE:
                upload_batch(pd.concat(batch_frames, ignore_index=True))
                batch_frames = []

        if batch_frames:
            upload_batch(pd.concat(batch_frames, ignore_index=True))

    finally:
        driver.quit()

    print("\n🏁 Backfill complete")
    print(f"✅ Successful race pages: {processed}")
    print(f"❌ Failed/empty race pages: {len(failed_urls)}")

    if failed_urls:
        print("\nFailed URLs:")
        for url in failed_urls[:50]:
            print(url)


if __name__ == "__main__":
    main()
