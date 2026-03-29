"""
NJ Foreclosure Bot - Essex County
Scrapes Essex County PRESS for lis pendens foreclosure filings,
looks up addresses via NJ MOD-IV tax data,
outputs to CSV and uploads to Google Sheets.
"""
import csv
import json
import os
import re
import time
import requests
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

import gspread
from google.oauth2.service_account import Credentials

LOOKBACK_DAYS = int(os.environ.get('LOOKBACK_DAYS', '30'))
OUTPUT_CSV = os.environ.get('OUTPUT_CSV', 'essex_foreclosures.csv')
SHEET_ID = os.environ.get('GOOGLE_SHEET_ID', '1D8fNy-v6_iMi_5jl6IuTZlS6-6xPYEh9HgIGJCHJ0yg')
MODIV_API = 'https://data.nj.gov/resource/9qqx-mnbd.json'

JUNK_PATTERNS = [
    'direct party', 'indirect party', 'instrument #', 'recorded',
    'town name', 'register of deeds', 'property records', 'terms of use',
    'site compatible', 'records search', 'rel 20', 'sunrise',
    'search results', 'no records', 'page of', 'home', 'faqs',
    'contact us', 'view image',
]

def is_junk_row(text):
    t = text.lower().strip()
    if not t:
        return True
    if len(t) > 200:
        return True
    for pat in JUNK_PATTERNS:
        if pat in t:
            return True
    return False

def is_valid_date(s):
    return bool(re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', s.strip()))

def lookup_address_by_block_lot(block, lot):
    try:
        if not block or block == 'N/A' or not lot or lot == 'N/A':
            return None
        params = {
            '$where': f"county_code='07' AND block='{block.zfill(5)}' AND lot='{lot.zfill(4)}'",
            '$limit': 1
        }
        r = requests.get(MODIV_API, params=params, timeout=10)
        if r.status_code == 200 and r.json():
            rec = r.json()[0]
            street = rec.get('property_location', '').strip()
            city = rec.get('property_city', '').strip().title()
            zipcode = rec.get('zip_code', '').strip()[:5]
            if street:
                return {'address': street, 'city': city, 'zip': zipcode}
    except Exception as e:
        print(f'  Address lookup error (block/lot): {e}')
    return None

def lookup_address_by_name(last_name, municipality):
    try:
        params = {
            '$where': (
                f"county_code='07' "
                f"AND upper(property_owner) LIKE '%{last_name.upper()}%' "
                f"AND upper(municipality_name) LIKE '%{municipality.upper()}%'"
            ),
            '$limit': 1
        }
        r = requests.get(MODIV_API, params=params, timeout=10)
        if r.status_code == 200 and r.json():
            rec = r.json()[0]
            street = rec.get('property_location', '').strip()
            city = rec.get('property_city', '').strip().title()
            zipcode = rec.get('zip_code', '').strip()[:5]
            if street:
                return {'address': street, 'city': city, 'zip': zipcode}
    except Exception as e:
        print(f'  Address lookup error (name): {e}')
    return None

def enrich_address(record):
    block = record.get('block', '').strip()
    lot = record.get('lot', '').strip()
    city = record.get('city', '').strip()
    name = record.get('name', '').strip()
    if block and lot and block != 'N/A' and lot != 'N/A':
        result = lookup_address_by_block_lot(block, lot)
        if result:
            record['address'] = result['address']
            record['city'] = result.get('city') or city
            record['zip'] = result['zip']
            return record
    if name and city:
        last_name = name.split()[-1] if name.split() else name
        result = lookup_address_by_name(last_name, city)
        if result:
            record['address'] = result['address']
            record['city'] = result.get('city') or city
            record['zip'] = result['zip']
    return record

def reset_to_search(page, url):
    page.goto(url, wait_until='domcontentloaded')
    page.wait_for_timeout(2000)
    for sel in ['input[value="Close"]', 'button:has-text("Close")',
                'input[value="I Agree"]', 'button:has-text("I Agree")']:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(500)
                break
        except Exception:
            pass
    try:
        page.click('text=By Document Type', timeout=5000)
        page.wait_for_timeout(500)
    except Exception:
        pass

def get_data_rows(page):
    grid_selectors = [
        '#ctl00_ContentPlaceHolder1_GridView1',
        '#ctl00_ContentPlaceHolder1_gvResults',
        '#ctl00_ContentPlaceHolder1_dgResults',
        'table[id*="GridView"]',
        'table[id*="gv"]',
        'table[id*="Results"]',
    ]
    for sel in grid_selectors:
        try:
            grid = page.query_selector(sel)
            if grid:
                rows = grid.query_selector_all('tr')
                data_rows = [r for r in rows if r.query_selector('td')]
                if data_rows:
                    print(f'  Found result grid via {sel}: {len(data_rows)} rows')
                    return data_rows
        except Exception:
            pass
    all_tables = page.query_selector_all('table')
    best_table = None
    best_count = 0
    for tbl in all_tables:
        rows = tbl.query_selector_all('tr')
        data_rows = [r for r in rows if r.query_selector('td')]
        has_date = False
        for row in data_rows[:10]:
            cells = row.query_selector_all('td')
            for cell in cells:
                txt = cell.inner_text().strip()
                if is_valid_date(txt):
                    has_date = True
                    break
            if has_date:
                break
        if has_date and len(data_rows) > best_count:
            best_count = len(data_rows)
            best_table = data_rows
    if best_table:
        print(f'  Found result table by date-scan: {len(best_table)} rows')
        return best_table
    all_rows = page.query_selector_all('table tr')
    rows = [r for r in all_rows if r.query_selector('td')]
    print(f'  Fallback: using all {len(rows)} rows from all tables')
    return rows

def parse_row(texts):
    if len(texts) < 6:
        return None
    date_col = None
    for i, t in enumerate(texts):
        if is_valid_date(t):
            date_col = i
            break
    if date_col is None:
        return None
    if date_col < 3 or date_col + 1 >= len(texts):
        return None
    recorded_date = texts[date_col].strip()
    direct_party = texts[date_col - 3].strip()
    indirect_party = texts[date_col - 2].strip()
    instrument_num = texts[date_col - 1].strip()
    town = texts[date_col + 1].strip() if len(texts) > date_col + 1 else ''
    block = texts[date_col + 2].strip() if len(texts) > date_col + 2 else ''
    lot = texts[date_col + 3].strip() if len(texts) > date_col + 3 else ''
    if is_junk_row(direct_party):
        return None
    if not direct_party or len(direct_party) < 2:
        return None
    if re.match(r'^[\d\s]+$', direct_party):
        return None
    return {
        'name': direct_party.title(),
        'lender': indirect_party.title(),
        'instrument_number': instrument_num,
        'filing_date': recorded_date,
        'city': town.title(),
        'block': block.replace('N/A', '').strip(),
        'lot': lot.replace('N/A', '').strip(),
        'address': '',
        'state': 'NJ',
        'zip': '',
        'county': 'Essex',
    }

def scrape_essex(page, from_date_str, to_date_str):
    records = []
    url = 'https://press.essexregister.com/prodpress/clerk/ClerkHome.aspx?op=basic'
    print('Navigating to Essex PRESS...')
    reset_to_search(page, url)
    doc_types = [
        ('23', 'LIS PENDENS FORECLOSURE'),
        ('21', 'LIS PENDENS IN REM'),
        ('24', 'LIS PENDENS RECOVERY'),
        ('25', 'LIS PENDENS FORECLOSURE AND RECOVERY'),
    ]
    for doc_val, doc_label in doc_types:
        print(f'\n  Searching: {doc_label}...')
        try:
            page.select_option('#ctl00_ContentPlaceHolder1_ddlDocTypeTab2', doc_val)
            page.wait_for_timeout(300)
            page.fill('#ctl00_ContentPlaceHolder1_txtFromTab2', from_date_str)
            page.fill('#ctl00_ContentPlaceHolder1_txtToTab2', to_date_str)
            page.click('#ctl00_ContentPlaceHolder1_btnSearchTab2')
            page.wait_for_timeout(4000)
        except Exception as e:
            print(f'  Search error for {doc_label}: {e}')
            reset_to_search(page, url)
            continue
        body = page.inner_text('body').lower()
        no_results = any(kw in body for kw in [
            'no records', 'returned 0', 'no results found', '0 records'
        ])
        if no_results:
            print(f'  {doc_label}: 0 results')
            reset_to_search(page, url)
            continue
        page_num = 1
        consecutive_empty = 0
        while True:
            page.wait_for_timeout(1500)
            rows = get_data_rows(page)
            page_records = 0
            for row in rows:
                try:
                    cells = row.query_selector_all('td')
                    if len(cells) < 5:
                        continue
                    texts = [c.inner_text().strip() for c in cells]
                    rec = parse_row(texts)
                    if rec:
                        rec['doc_type'] = doc_label
                        records.append(rec)
                        page_records += 1
                except Exception as e:
                    print(f'  Row parse error: {e}')
            print(f'  Page {page_num}: captured {page_records} records')
            if page_records == 0:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    break
            else:
                consecutive_empty = 0
            next_link = None
            for sel in ['a:has-text("Next")', 'a:has-text(">>")', 'a:has-text(">")',
                        'input[value="Next"]', 'input[value=">"]']:
                try:
                    el = page.query_selector(sel)
                    if el and el.is_visible():
                        next_link = el
                        break
                except Exception:
                    pass
            if next_link:
                try:
                    next_link.click()
                    page.wait_for_timeout(3000)
                    page_num += 1
                except Exception as e:
                    print(f'  Pagination error: {e}')
                    break
            else:
                print(f'  No more pages for {doc_label}')
                break
        reset_to_search(page, url)
    return records

def write_csv(records, filename):
    if not records:
        print('No records to write.')
        return
    fieldnames = ['name', 'address', 'city', 'state', 'zip', 'county',
                  'filing_date', 'lender', 'instrument_number', 'doc_type', 'block', 'lot']
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f'\nWrote {len(records)} records to {filename}')

def upload_to_sheets(records, sheet_id):
    if not records:
        print('No records to upload to Google Sheets.')
        return
    creds_json = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    if not creds_json:
        print('No GOOGLE_SERVICE_ACCOUNT_JSON env var set, skipping Sheets upload.')
        return
    try:
        creds_dict = json.loads(creds_json)
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        ws = sh.sheet1
        existing_data = ws.get_all_values()
        if existing_data:
            header = existing_data[0]
            try:
                inst_col = header.index('instrument_number')
                existing_instruments = {row[inst_col] for row in existing_data[1:] if len(row) > inst_col}
            except ValueError:
                existing_instruments = set()
        else:
            existing_instruments = set()
        fieldnames = ['name', 'address', 'city', 'state', 'zip', 'county',
                      'filing_date', 'lender', 'instrument_number', 'doc_type', 'block', 'lot']
        if not existing_data:
            ws.append_row(fieldnames)
        new_rows = []
        for rec in records:
            inst = rec.get('instrument_number', '')
            if inst not in existing_instruments:
                new_rows.append([rec.get(f, '') for f in fieldnames])
                existing_instruments.add(inst)
        if new_rows:
            ws.append_rows(new_rows, value_input_option='RAW')
            print(f'Uploaded {len(new_rows)} new records to Google Sheets.')
        else:
            print('No new records to upload (all already exist in sheet).')
    except Exception as e:
        print(f'Google Sheets upload error: {e}')
        raise

def main():
    print(f'\n=== NJ Foreclosure Bot (Essex County) ===')
    print(f'Date: {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print(f'Lookback: {LOOKBACK_DAYS} day(s)')
    print('=' * 45)
    today = datetime.now()
    from_date = today - timedelta(days=LOOKBACK_DAYS)
    from_str = from_date.strftime('%m/%d/%Y')
    to_str = today.strftime('%m/%d/%Y')
    print(f'Date range: {from_str} to {to_str}\n')
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        )
        ctx = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36'
        )
        page = ctx.new_page()
        records = scrape_essex(page, from_str, to_str)
        browser.close()
    print(f'\nScraped {len(records)} raw records')
    seen = set()
    unique = []
    for r in records:
        key = r.get('instrument_number') or f"{r['name']}-{r['filing_date']}"
        if key not in seen:
            seen.add(key)
            unique.append(r)
    print(f'Unique records: {len(unique)}')
    print('\nLooking up addresses via NJ MOD-IV tax data...')
    for rec in unique:
        enrich_address(rec)
        time.sleep(0.3)
    write_csv(unique, OUTPUT_CSV)
    print('\nUploading to Google Sheets...')
    upload_to_sheets(unique, SHEET_ID)
    print('\nDone!')

if __name__ == '__main__':
    main()
