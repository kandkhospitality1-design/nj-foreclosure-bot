"""
NJ Foreclosure Bot - Simplified (Essex County)
Scrapes Essex County PRESS for lis pendens foreclosure filings,
looks up addresses via NJ MOD-IV tax data, outputs to CSV.
"""

import csv
import os
import time
import requests
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

# ── Config ──
LOOKBACK_DAYS = int(os.environ.get('LOOKBACK_DAYS', '30'))
OUTPUT_CSV = os.environ.get('OUTPUT_CSV', 'essex_foreclosures.csv')

MODIV_API = 'https://data.nj.gov/resource/9qqx-mnbd.json'

# ── Address lookup via NJ MOD-IV ──

def lookup_address_by_block_lot(block, lot):
      """Query NJ MOD-IV for street address given block/lot in Essex County."""
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
      """Fallback: search MOD-IV by owner name + municipality in Essex."""
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
      """Fill in address/zip using block/lot or name fallback."""
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
                            print(f"  Found address via block/lot: {result['address']}, {result['city']}")
                            return record

          if name and city:
                    last_name = name.split()[-1] if name.split() else name
                    result = lookup_address_by_name(last_name, city)
                    if result:
                                  record['address'] = result['address']
                                  record['city'] = result.get('city') or city
                                  record['zip'] = result['zip']
                                  print(f"  Found address via name: {result['address']}, {result['city']}")
                                  return record

                print(f'  No address found for {name} in {city}')
    return record


# ── Essex County PRESS scraper ──

def scrape_essex(page, from_date_str, to_date_str):
      """
          Scrape Essex County PRESS for lis pendens foreclosure filings.
              Returns list of dicts with filing data.
                  """
    records = []
    url = 'https://press.essexregister.com/prodpress/clerk/ClerkHome.aspx?op=basic'
    print(f'Navigating to Essex PRESS...')
    page.goto(url, wait_until='domcontentloaded')
    page.wait_for_timeout(3000)

    # Dismiss disclaimer if present
    try:
              close_btn = page.query_selector('input[value="Close"], button:has-text("Close")')
              if close_btn:
                            close_btn.click()
                            page.wait_for_timeout(500)
    except Exception:
        pass

    # Click "By Document Type" tab
    page.click('text=By Document Type')
    page.wait_for_timeout(500)

    # Search each lis pendens doc type
    doc_types = [
              ('23', 'LIS PENDENS FORECLOSURE'),
              ('21', 'LIS PENDENS IN REM'),
              ('24', 'LIS PENDENS RECOVERY'),
              ('25', 'LIS PENDENS FORECLOSURE AND RECOVERY'),
    ]

    for doc_val, doc_label in doc_types:
              print(f'\n  Searching: {doc_label}...')
              page.select_option('#ctl00_ContentPlaceHolder1_ddlDocTypeTab2', doc_val)
              page.wait_for_timeout(300)
              page.fill('#ctl00_ContentPlaceHolder1_txtFromTab2', from_date_str)
              page.fill('#ctl00_ContentPlaceHolder1_txtToTab2', to_date_str)

        page.click('#ctl00_ContentPlaceHolder1_btnSearchTab2')
        page.wait_for_timeout(3000)

        body = page.inner_text('body')
        if 'No records' in body or 'returned 0' in body.lower():
                      print(f'    {doc_label}: 0 results')
                      page.goto(url, wait_until='domcontentloaded')
                      page.wait_for_timeout(1500)
                      try:
                                        close_btn = page.query_selector('input[value="Close"], button:has-text("Close")')
                                        if close_btn:
                                                              close_btn.click()
                                                              page.wait_for_timeout(300)
                      except Exception:
                                        pass
                                    page.click('text=By Document Type')
            page.wait_for_timeout(500)
            continue

        # Paginate through results
        page_num = 1
        while True:
                      all_rows = page.query_selector_all('table tr')
            rows = [r for r in all_rows if r.query_selector('td')]
            print(f'    Page {page_num}: {len(rows)} rows')

            for row in rows:
                              try:
                                                    cells = row.query_selector_all('td')
                                                    if len(cells) < 4:
                                                                              continue
                                                                          texts = [c.inner_text().strip() for c in cells]

                                  direct_party = texts[1] if len(texts) > 1 else ''
                    indirect_party = texts[2] if len(texts) > 2 else ''
                    instrument_num = texts[3] if len(texts) > 3 else ''
                    recorded_date = texts[4] if len(texts) > 4 else ''
                    town = texts[5] if len(texts) > 5 else ''
                    block = texts[6] if len(texts) > 6 else ''
                    lot = texts[7] if len(texts) > 7 else ''

                    if not direct_party or not recorded_date:
                                              continue
                                          if 'direct party' in direct_party.lower():
                                                                    continue

                    records.append({
                                              'name': direct_party.title(),
                                              'lender': indirect_party,
                                              'instrument_number': instrument_num,
                                              'filing_date': recorded_date,
                                              'city': town.title(),
                                              'block': block.replace('N/A', '').strip(),
                                              'lot': lot.replace('N/A', '').strip(),
                                              'address': '',
                                              'state': 'NJ',
                                              'zip': '',
                                              'county': 'Essex',
                                              'doc_type': doc_label,
                    })
except Exception as e:
                    print(f'    Row parse error: {e}')

            # Check for next page
            next_link = page.query_selector('a:has-text("Next"), a[href*="page="]')
            if next_link:
                              next_link.click()
                page.wait_for_timeout(2000)
                page_num += 1
else:
                break

        # Go back to search for next doc type
        page.goto(url, wait_until='domcontentloaded')
        page.wait_for_timeout(1500)
        try:
                      close_btn = page.query_selector('input[value="Close"], button:has-text("Close")')
            if close_btn:
                              close_btn.click()
                page.wait_for_timeout(300)
except Exception:
            pass
        page.click('text=By Document Type')
        page.wait_for_timeout(500)

    return records


# ── Write results to CSV ──

def write_csv(records, filename):
      """Write records to a CSV file."""
    if not records:
              print('No records to write.')
        return

    fieldnames = [
              'name', 'address', 'city', 'state', 'zip', 'county',
              'filing_date', 'lender', 'instrument_number', 'doc_type',
              'block', 'lot'
    ]

    with open(filename, 'w', newline='') as f:
              writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f'\nWrote {len(records)} records to {filename}')


# ── Main ──

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
                            args=['--no-sandbox', '--disable-setuid-sandbox',
                                                    '--disable-dev-shm-usage']
              )
        ctx = browser.new_context(
                      user_agent=(
                                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                        'AppleWebKit/537.36 (KHTML, like Gecko) '
                                        'Chrome/120.0.0.0 Safari/537.36'
                      )
        )
        page = ctx.new_page()

        records = scrape_essex(page, from_str, to_str)
        browser.close()

    print(f'\nScraped {len(records)} raw records')

    # Deduplicate by instrument number
    seen = set()
    unique = []
    for r in records:
              key = r.get('instrument_number') or f"{r['name']}-{r['filing_date']}"
        if key not in seen:
                      seen.add(key)
            unique.append(r)
    print(f'Unique records: {len(unique)}')

    # Enrich with addresses from MOD-IV
    print('\nLooking up addresses via NJ MOD-IV tax data...')
    for rec in unique:
              enrich_address(rec)
        time.sleep(0.3)  # be polite to the API

    # Write to CSV
    write_csv(unique, OUTPUT_CSV)
    print('\nDone!')


if __name__ == '__main__':
      main()
