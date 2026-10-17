"""Repair missing historical pre-race meeting conditions. DRY RUN by default.

Uses Sporting Life's results-day meeting summaries, then appends complete
copies of existing horse rows with only missing Going/Surface filled.
Never changes the race spine, horse attributes, or post-race data.
"""
import argparse
import re
import time
from datetime import date, timedelta

from bs4 import BeautifulSoup
from google.cloud import bigquery
from google.oauth2 import service_account
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

PROJECT = 'horseracing-pacey32-github'
TABLE = f'{PROJECT}.horseracescrape.Scrape_PreRace'
MISSING = "(x IS NULL OR UPPER(TRIM(x)) IN ('N/A','NA',''))"

def missing(column):
    return MISSING.replace('x', column)

def driver_setup():
    options = Options()
    for arg in ['--headless=new','--no-sandbox','--disable-dev-shm-usage','--window-size=1920,1080']:
        options.add_argument(arg)
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def normalise(s):
    return re.sub(r'[^a-z0-9]', '', str(s).lower())

def meeting_conditions(html):
    soup = BeautifulSoup(html, 'html.parser')
    meetings = soup.select('[data-testid="meeting-summary"]')
    results = []
    for meeting in meetings:
        name_el = meeting.select_one('[data-testid="course-name"]')
        if not name_el:
            continue
        name = name_el.get_text(' ', strip=True)
        text = meeting.get_text(' ', strip=True)
        # Capture only the summary/header portion, before race titles.
        header = text[:min(len(text), 500)]
        going_match = re.search(r'Going\s*:\s*(.*?)\s+Surface\s*:', header, re.I)
        surface_match = re.search(r'Surface\s*:\s*(Turf|Polytrack|Tapeta|Fibresand|All[ -]?weather|Dirt|Sand|Grass)\b', header, re.I)
        results.append((name, going_match.group(1).strip() if going_match else None,
                        surface_match.group(1).strip() if surface_match else None))
    return results

def source_counts(client, day):
    sql = f'''SELECT RaceLocation AS course, COUNT(*) AS rows,
      COUNTIF({missing('RaceGoing')}) AS missing_going,
      COUNTIF({missing('RaceSurface')}) AS missing_surface
      FROM `{TABLE}` WHERE SAFE.PARSE_DATE('%d/%m/%Y', RaceDate)=@day
      QUALIFY FALSE'''
    # Group after deduplication so count matches the current view, not raw append history.
    sql = f'''WITH ranked AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY SourceURL,HorseName ORDER BY load_timestamp DESC) rn
      FROM `{TABLE}` WHERE SAFE.PARSE_DATE('%d/%m/%Y',RaceDate)=@day
    ) SELECT RaceLocation course, COUNT(*) rows,
      COUNTIF({missing('RaceGoing')}) missing_going,
      COUNTIF({missing('RaceSurface')}) missing_surface
      FROM ranked WHERE rn=1 GROUP BY course ORDER BY course'''
    return list(client.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter('day','DATE',day)])).result())

def append_repair(client, day, course, going, surface):
    # Enumerate actual base-table columns so that the insert exactly matches schema.
    schema = client.get_table(TABLE).schema
    cols = [field.name for field in schema]
    names = ', '.join(f'`{c}`' for c in cols)
    expressions = []
    for col in cols:
        if col == 'RaceGoing':
            expressions.append(f'IF({missing("s.RaceGoing")}, @going, s.RaceGoing)')
        elif col == 'RaceSurface':
            expressions.append(f'IF({missing("s.RaceSurface")}, @surface, s.RaceSurface)')
        elif col == 'load_timestamp':
            expressions.append('CURRENT_TIMESTAMP()')
        else:
            expressions.append(f's.`{col}`')
    sql = f'''INSERT INTO `{TABLE}` ({names})
      SELECT {', '.join(expressions)}
      FROM (SELECT *, ROW_NUMBER() OVER (
        PARTITION BY SourceURL,HorseName ORDER BY load_timestamp DESC) AS rn
        FROM `{TABLE}` WHERE SAFE.PARSE_DATE('%d/%m/%Y',RaceDate)=@day) s
      WHERE rn=1 AND LOWER(TRIM(RaceLocation))=LOWER(TRIM(@course))
        AND ((@going IS NOT NULL AND {missing('s.RaceGoing')})
          OR (@surface IS NOT NULL AND {missing('s.RaceSurface')}))'''
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter('day','DATE',day),
        bigquery.ScalarQueryParameter('course','STRING',course),
        bigquery.ScalarQueryParameter('going','STRING',going),
        bigquery.ScalarQueryParameter('surface','STRING',surface),
    ]))
    job.result()
    return job.num_dml_affected_rows

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--end-date', help='YYYY-MM-DD; optional')
    parser.add_argument('--apply', action='store_true', help='Append repairs to BigQuery; default dry-run')
    args=parser.parse_args()
    start=date.fromisoformat(args.date)
    end=date.fromisoformat(args.end_date or args.date)
    if end<start or (end-start).days>60:
        parser.error('Date range must be 1–61 days')
    creds=service_account.Credentials.from_service_account_file('key.json')
    client=bigquery.Client(project=PROJECT, credentials=creds)
    driver=driver_setup()
    try:
        day=start
        while day<=end:
            url=f'https://www.sportinglife.com/racing/results/{day.isoformat()}'
            print(f'\nDATE {day}  URL {url}',flush=True)
            driver.get(url)
            time.sleep(5)
            extracted=meeting_conditions(driver.page_source)
            counts=source_counts(client,day)
            print(f'Found {len(extracted)} website meetings; {len(counts)} BigQuery courses')
            matches={normalise(name): (name,going,surface) for name,going,surface in extracted}
            for row in counts:
                course=row.course
                item=matches.get(normalise(course))
                if not item:
                    print(f'  SKIP {course}: no matching meeting on page')
                    continue
                _,going,surface=item
                print(f'  {course}: missing going={row.missing_going}, surface={row.missing_surface}; recovered going={going!r}, surface={surface!r}')
                if not going and not surface:
                    print('    SKIP: no conditions extracted')
                    continue
                if args.apply:
                    changed=append_repair(client,day,course,going,surface)
                    print(f'    APPENDED {changed} complete horse records')
                else:
                    print('    DRY RUN: no writes')
            day+=timedelta(days=1)
    finally:
        driver.quit()

if __name__=='__main__':
    main()
