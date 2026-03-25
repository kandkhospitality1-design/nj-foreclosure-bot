"""
NJ Foreclosure Bot - v2
Sources: Essex County PRESS, Union County PRESS, Passaic County PRESS
Replaces: njlispendens.com (login broken)

Data collected per record:
  name, address, city, state, zip, county, filing_date, lender, instrument_number

Flow:
  1. Scrape county PRESS sites for today's LIS PENDENS FORECLOSURE filings
  2. For each record that has Block/Lot, look up street address via NJ MOD-IV tax API
  3. If no Block/Lot (Essex often returns N/A), fall back to name+city lookup on tax API
  4. Create lead in ReSimpli → skip trace → enroll drip
"""

import os
import re
import json
import time
import requests
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

# ── Env vars (same names as before, NJ_USERNAME/NJ_PASSWORD no longer needed) ──
RESIMPLI_TOKEN   = os.environ['RESIMPLI_TOKEN']
SEEN_FILE        = 'seen_properties.json'
TEST_MODE        = os.environ.get('TEST_MODE', 'false').lower() == 'true'
LOOKBACK_DAYS    = int(os.environ.get('LOOKBACK_DAYS', '1'))   # default: today only

# ReSimpli drip IDs (unchanged from original)
DRIP_HOT  = '69c2f816a121be22d81a6c85'
DRIP_WARM = '69c2f97068818e6c3fa39cd5'
DRIP_COLD = '69c30c665ea684747e339fa3'

RESIMPLI_HEADERS = {
    'Authorization': RESIMPLI_TOKEN,
    'Content-Type': 'application/json'
}
RESIMPLI_BASE = 'https://live-api.resimpli.com/api/v4'

# ── County PRESS portal configs ──────────────────────────────────────────────
# Each entry: name, base_url, doc_type_value, uses_municipality_filter
COUNTIES = [
    {
        'name': 'Essex',
        'press_url': 'https://press.essexregister.com/prodpress/clerk/ClerkHome.aspx?op=basic',
        'doc_type_value': '23',        # LIS PENDENS FORECLOSURE
        'tab': 'Tab2',                 # "By Document Type" tab identifiers
        'municipality_select_id': 'ctl00_ContentPlaceHolder1_ddlMunTab2',
        'doctype_select_id':      'ctl00_ContentPlaceHolder1_ddlDocTypeTab2',
        'from_date_id':           'ctl00_ContentPlaceHolder1_txtFromTab2',
        'to_date_id':             'ctl00_ContentPlaceHolder1_txtToTab2',
        'search_button_tab':      'Tab2',
    },
    {
        'name': 'Union',
        'press_url': 'https://clerk.ucnj.org/UCPA/DocIndex?s=type',
        'type': 'union',   # handled separately - different UI
    },
    {
        'name': 'Passaic',
        'press_url': 'https://passaiccountyclerk.com/LandRecords/',
        'type': 'passaic',  # handled separately
    },
]

# ── NJ MOD-IV / tax assessor API (free, no key needed) ───────────────────────
NJ_TAX_API = 'https://api.njpropertyrecords.com/v1/properties'   # community endpoint
# Fallback: direct county assessor lookups via NJ's MODIV dataset on data.nj.gov
MODIV_API  = 'https://data.nj.gov/resource/9qqx-mnbd.json'


# ─────────────────────────────────────────────────────────────────────────────
#  Scoring (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────

def equity_score(ev, lb):
    if not ev: return 0
    p = (ev - lb) / ev * 100
    if p >= 50: return 35
    if p >= 40: return 30
    if p >= 30: return 22
    if p >= 20: return 14
    if p >= 10: return 7
    return 0

def distress_score(days, tax=False, absentee=False, vacant=False):
    pts = 10 if days <= 7 else 7 if days <= 30 else 4 if days <= 90 else 0
    if tax:      pts += 8
    if absentee: pts += 5
    if vacant:   pts += 2
    return min(pts, 25)

def market_score(city):
    hot  = {'newark', 'paterson', 'elizabeth', 'irvington'}
    warm = {'east orange', 'belleville', 'bloomfield', 'nutley', 'west orange'}
    c = city.lower().strip()
    if c in hot:  return 20
    if c in warm: return 15
    return 10

def deal_score(has_phone=False, prop_type='SFR', year_built=None, no_bankruptcy=True):
    pts = 0
    if has_phone:  pts += 5
    if prop_type == 'SFR': pts += 5
    elif prop_type in ('2-4', 'duplex', 'triplex'): pts += 4
    if year_built and year_built < 1980: pts += 3
    if no_bankruptcy: pts += 3
    return min(pts, 20)

def kps_score(ev, lb, days, city, **kwargs):
    return (equity_score(ev, lb) + distress_score(days, **kwargs)
            + market_score(city) + deal_score())


# ─────────────────────────────────────────────────────────────────────────────
#  Address lookup via NJ MOD-IV open data
# ─────────────────────────────────────────────────────────────────────────────

def lookup_address_by_block_lot(county, block, lot):
    """Query NJ MOD-IV dataset for street address given county/block/lot."""
    try:
        county_code_map = {
            'Atlantic': '01', 'Bergen': '02', 'Burlington': '03',
            'Camden': '04', 'Cape May': '05', 'Cumberland': '06',
            'Essex': '07', 'Gloucester': '08', 'Hudson': '09',
            'Hunterdon': '10', 'Mercer': '11', 'Middlesex': '12',
            'Monmouth': '13', 'Morris': '14', 'Ocean': '15',
            'Passaic': '16', 'Salem': '17', 'Somerset': '18',
            'Sussex': '19', 'Union': '20', 'Warren': '21',
        }
        cc = county_code_map.get(county, '')
        if not cc or not block or block == 'N/A':
            return None

        params = {
            '$where': f"county_code='{cc}' AND block='{block.zfill(5)}' AND lot='{lot.zfill(4)}'",
            '$limit': 1
        }
        r = requests.get(MODIV_API, params=params, timeout=10)
        if r.status_code == 200 and r.json():
            rec = r.json()[0]
            street   = rec.get('property_location', '').strip()
            city     = rec.get('property_city',     '').strip().title()
            zip_code = rec.get('zip_code',          '').strip()[:5]
            if street:
                return {'address': street, 'city': city, 'zip': zip_code}
    except Exception as e:
        print(f'  Address lookup error (block/lot): {e}')
    return None


def lookup_address_by_name(county, last_name, municipality):
    """Fallback: search MOD-IV by owner name + municipality."""
    try:
        params = {
            '$where': (f"county_code='{_county_code(county)}' "
                       f"AND upper(property_owner) LIKE '%{last_name.upper()}%' "
                       f"AND upper(municipality_name) LIKE '%{municipality.upper()}%'"),
            '$limit': 1
        }
        r = requests.get(MODIV_API, params=params, timeout=10)
        if r.status_code == 200 and r.json():
            rec = r.json()[0]
            street   = rec.get('property_location', '').strip()
            city     = rec.get('property_city',     '').strip().title()
            zip_code = rec.get('zip_code',          '').strip()[:5]
            if street:
                return {'address': street, 'city': city, 'zip': zip_code}
    except Exception as e:
        print(f'  Address lookup error (name): {e}')
    return None


def _county_code(county):
    m = {'Atlantic':'01','Bergen':'02','Burlington':'03','Camden':'04',
         'Cape May':'05','Cumberland':'06','Essex':'07','Gloucester':'08',
         'Hudson':'09','Hunterdon':'10','Mercer':'11','Middlesex':'12',
         'Monmouth':'13','Morris':'14','Ocean':'15','Passaic':'16',
         'Salem':'17','Somerset':'18','Sussex':'19','Union':'20','Warren':'21'}
    return m.get(county, '07')


# ─────────────────────────────────────────────────────────────────────────────
#  Essex County PRESS scraper (primary - confirmed working)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_essex(page, from_date_str, to_date_str):
    """
    Scrapes Essex County PRESS for LIS PENDENS FORECLOSURE filings.
    Returns list of dicts: {name, city, block, lot, date, lender, instrument_number, county}
    """
    records = []
    url = 'https://press.essexregister.com/prodpress/clerk/ClerkHome.aspx?op=basic'
    print(f'  Navigating to Essex PRESS...')
    page.goto(url, wait_until='domcontentloaded')
    page.wait_for_timeout(2000)

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

    # Select document type: LIS PENDENS FORECLOSURE (value=23)
    page.select_option('#ctl00_ContentPlaceHolder1_ddlDocTypeTab2', '23')
    page.wait_for_timeout(300)

    # Set date range
    page.fill('#ctl00_ContentPlaceHolder1_txtFromTab2', from_date_str)
    page.fill('#ctl00_ContentPlaceHolder1_txtToTab2',   to_date_str)

    # Also grab LIS PENDENS IN REM (value=21) and LIS PENDENS RECOVERY (value=24)
    # by running separate searches - start with FORECLOSURE (23)
    doc_types = [
        ('23', 'LIS PENDENS FORECLOSURE'),
        ('21', 'LIS PENDENS IN REM'),
        ('24', 'LIS PENDENS RECOVERY'),
        ('25', 'LIS PENDENS FORECLOSURE AND RECOVERY'),
    ]

    for doc_val, doc_label in doc_types:
        page.select_option('#ctl00_ContentPlaceHolder1_ddlDocTypeTab2', doc_val)
        page.wait_for_timeout(300)
        page.fill('#ctl00_ContentPlaceHolder1_txtFromTab2', from_date_str)
        page.fill('#ctl00_ContentPlaceHolder1_txtToTab2',   to_date_str)

        # Hit search
        page.click('#ctl00_ContentPlaceHolder1_btnSearchTab2')
        page.wait_for_timeout(3000)

        # Check for results
        body = page.inner_text('body')
        if 'No records' in body or 'returned 0' in body.lower():
            print(f'    Essex {doc_label}: 0 results')
            page.go_back()
            page.wait_for_timeout(1000)
            page.click('text=By Document Type')
            page.wait_for_timeout(500)
            continue

        # Paginate through all result pages
        page_num = 1
        while True:
            rows = page.query_selector_all('tr.SearchResult, tr[class*="result"], .search-result-row')
            if not rows:
                # Try generic table rows (skip header)
                all_rows = page.query_selector_all('table tr')
                rows = [r for r in all_rows if r.query_selector('td')]

            print(f'    Essex {doc_label} page {page_num}: {len(rows)} rows')

            for row in rows:
                try:
                    cells = row.query_selector_all('td')
                    if len(cells) < 4:
                        continue
                    texts = [c.inner_text().strip() for c in cells]

                    # Column order from PRESS: Type | Direct Party | Indirect Party |
                    #                          Instrument# | Recorded | Town | Block | Lot | Book | Page | VIEW
                    doc_type_cell   = texts[0] if len(texts) > 0 else ''
                    direct_party    = texts[1] if len(texts) > 1 else ''
                    indirect_party  = texts[2] if len(texts) > 2 else ''
                    instrument_num  = texts[3] if len(texts) > 3 else ''
                    recorded_date   = texts[4] if len(texts) > 4 else ''
                    town            = texts[5] if len(texts) > 5 else ''
                    block           = texts[6] if len(texts) > 6 else ''
                    lot             = texts[7] if len(texts) > 7 else ''

                    if not direct_party or not recorded_date:
                        continue

                    # Skip header rows
                    if 'direct party' in direct_party.lower():
                        continue

                    records.append({
                        'county':            'Essex',
                        'name':              direct_party.title(),
                        'lender':            indirect_party,
                        'instrument_number': instrument_num,
                        'date':              recorded_date,
                        'city':              town.title(),
                        'block':             block.replace('N/A', '').strip(),
                        'lot':               lot.replace('N/A', '').strip(),
                        'address':           '',   # filled in by address lookup
                        'zip':               '',
                        'state':             'NJ',
                        'doc_type':          doc_label,
                    })
                except Exception as e:
                    print(f'    Row parse error: {e}')

            # Check for next page link
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


# ─────────────────────────────────────────────────────────────────────────────
#  Union County scraper
# ─────────────────────────────────────────────────────────────────────────────

def scrape_union(page, from_date_str, to_date_str):
    """Scrapes Union County public land records for lis pendens."""
    records = []
    url = 'https://clerk.ucnj.org/UCPA/DocIndex?s=type'
    print(f'  Navigating to Union County PRESS...')
    try:
        page.goto(url, wait_until='domcontentloaded', timeout=15000)
        page.wait_for_timeout(2000)

        # Select document type - look for lis pendens option
        doc_select = page.query_selector('select[name*="type"], select[id*="type"], select')
        if doc_select:
            options = page.eval_on_selector(
                'select', 'el => Array.from(el.options).map(o => ({v:o.value,t:o.text}))'
            )
            lp_option = next(
                (o for o in options if 'lis pendens' in o['t'].lower()), None
            )
            if lp_option:
                page.select_option('select', lp_option['v'])
                page.wait_for_timeout(300)

        # Set date fields
        from_field = page.query_selector('input[name*="from"], input[id*="from"], input[placeholder*="from"]')
        to_field   = page.query_selector('input[name*="to"],   input[id*="to"],   input[placeholder*="to"]')
        if from_field: from_field.fill(from_date_str)
        if to_field:   to_field.fill(to_date_str)

        # Submit
        page.click('input[type="submit"], button[type="submit"], button:has-text("Search")')
        page.wait_for_timeout(3000)

        rows = page.query_selector_all('table tr')
        data_rows = [r for r in rows if r.query_selector('td')]
        print(f'    Union: {len(data_rows)} rows')

        for row in data_rows:
            try:
                cells = row.query_selector_all('td')
                texts = [c.inner_text().strip() for c in cells]
                if len(texts) < 3: continue
                records.append({
                    'county':            'Union',
                    'name':              texts[1].title() if len(texts) > 1 else '',
                    'lender':            texts[2] if len(texts) > 2 else '',
                    'instrument_number': texts[0] if texts else '',
                    'date':              texts[3] if len(texts) > 3 else '',
                    'city':              texts[4].title() if len(texts) > 4 else '',
                    'block':             '',
                    'lot':               '',
                    'address':           '',
                    'zip':               '',
                    'state':             'NJ',
                    'doc_type':          'LIS PENDENS',
                })
            except Exception as e:
                print(f'    Union row error: {e}')
    except Exception as e:
        print(f'  Union scrape failed: {e}')

    return records


# ─────────────────────────────────────────────────────────────────────────────
#  Passaic County scraper  
# ─────────────────────────────────────────────────────────────────────────────

def scrape_passaic(page, from_date_str, to_date_str):
    """Scrapes Passaic County land records for lis pendens."""
    records = []
    # Passaic uses a different system - try their online portal
    urls_to_try = [
        'https://passaiccountyclerk.com/LandRecords/',
        'https://clerk.co.passaic.nj.us/',
    ]
    for url in urls_to_try:
        try:
            print(f'  Trying Passaic at {url}...')
            page.goto(url, wait_until='domcontentloaded', timeout=10000)
            page.wait_for_timeout(2000)
            title = page.title()
            print(f'    Page title: {title}')
            # If we get a search form, attempt to use it
            search_input = page.query_selector('input[type="text"]')
            if search_input:
                print('    Passaic has search form - attempting lis pendens search')
                # Implementation depends on their specific UI
                # For now log and skip - Passaic is mostly in-person
                break
        except Exception as e:
            print(f'    Passaic URL failed: {e}')
    
    print('  Passaic County: online portal not reliably scrapeable - skipping (in-person only)')
    return records


# ─────────────────────────────────────────────────────────────────────────────
#  Address enrichment
# ─────────────────────────────────────────────────────────────────────────────

def enrich_address(record):
    """
    Attempts to fill record['address'] and record['zip'] using:
    1. Block/Lot → MOD-IV lookup
    2. Name + city → MOD-IV lookup (fallback)
    Returns the record (mutated in place).
    """
    county = record.get('county', 'Essex')
    block  = record.get('block', '').strip()
    lot    = record.get('lot', '').strip()
    city   = record.get('city', '').strip()
    name   = record.get('name', '').strip()

    # Try block/lot first
    if block and lot and block != 'N/A' and lot != 'N/A':
        result = lookup_address_by_block_lot(county, block, lot)
        if result:
            record['address'] = result['address']
            record['city']    = result['city'] or city
            record['zip']     = result['zip']
            print(f'    Address via block/lot: {result["address"]}, {result["city"]} {result["zip"]}')
            return record

    # Fallback: use last name + city
    if name and city:
        last_name = name.split()[-1] if name.split() else name
        result = lookup_address_by_name(county, last_name, city)
        if result:
            record['address'] = result['address']
            record['city']    = result['city'] or city
            record['zip']     = result['zip']
            print(f'    Address via name lookup: {result["address"]}, {result["city"]} {result["zip"]}')
            return record

    # No address found - leave blank (ReSimpli will skip-trace it)
    print(f'    No address found for {name} in {city} - will skip trace')
    return record


# ─────────────────────────────────────────────────────────────────────────────
#  ReSimpli integration (enhanced from original)
# ─────────────────────────────────────────────────────────────────────────────

def create_lead(prop):
    """Creates a lead in ReSimpli with full name + address data."""
    name_parts = prop.get('name', 'Unknown Owner').split()
    first = name_parts[0] if name_parts else 'Unknown'
    last  = ' '.join(name_parts[1:]) if len(name_parts) > 1 else 'Owner'

    payload = {
        'firstName':  first,
        'lastName':   last,
        'address':    prop.get('address', ''),
        'city':       prop.get('city', ''),
        'state':      'NJ',
        'zip':        prop.get('zip', ''),
        'leadSource': 'NJLisPendens-Direct',
        'notes': (
            f"County: {prop.get('county')} | "
            f"Instrument: {prop.get('instrument_number')} | "
            f"Filed: {prop.get('date')} | "
            f"Lender: {prop.get('lender')} | "
            f"Type: {prop.get('doc_type')} | "
            f"Score: {prop.get('score')}"
        )
    }

    r = requests.post(
        f'{RESIMPLI_BASE}/lead/save',
        headers=RESIMPLI_HEADERS,
        json=payload
    )
    if r.status_code == 200:
        data = r.json()
        lead_id = data.get('data', {}).get('_id') or data.get('_id')
        print(f'    Lead created: {lead_id} | {first} {last}')
        return lead_id
    print(f'    Lead create failed: {r.status_code} {r.text[:200]}')
    return None


def skip_trace(lead_id):
    r = requests.post(
        f'{RESIMPLI_BASE}/lead/leadSkipTrace',
        headers=RESIMPLI_HEADERS,
        json={'leadId': lead_id}
    )
    if r.status_code != 200:
        print(f'    Skip trace failed: {r.status_code} {r.text[:200]}')
    else:
        print(f'    Skip traced: {lead_id}')


def enroll_drip(lead_id, score):
    if score >= 80:
        drip_id = DRIP_HOT
        label   = 'HOT'
    elif score >= 65:
        drip_id = DRIP_WARM
        label   = 'WARM'
    elif score >= 45:
        drip_id = DRIP_COLD
        label   = 'COLD'
    else:
        print(f'    Score {score} below drip threshold - not enrolled')
        return

    r = requests.post(
        f'{RESIMPLI_BASE}/masterDrip/assignToLead',
        headers=RESIMPLI_HEADERS,
        json={'leadId': lead_id, 'masterDripId': drip_id}
    )
    if r.status_code != 200:
        print(f'    Drip enroll failed: {r.status_code} {r.text[:200]}')
    else:
        print(f'    Enrolled in {label} drip')


# ─────────────────────────────────────────────────────────────────────────────
#  Seen properties cache
# ─────────────────────────────────────────────────────────────────────────────

def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()


def save_seen(seen):
    with open(SEEN_FILE, 'w') as f:
        json.dump(list(seen), f)


# ─────────────────────────────────────────────────────────────────────────────
#  Main scrape orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def scrape_all():
    """Run all county scrapers and return combined, enriched property list."""
    all_records = []

    today     = datetime.now()
    from_date = today - timedelta(days=LOOKBACK_DAYS)
    from_str  = from_date.strftime('%m/%d/%Y')
    to_str    = today.strftime('%m/%d/%Y')

    print(f'Date range: {from_str} → {to_str}')

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
            ]
        )
        ctx = browser.new_context(
            user_agent=(
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            )
        )
        page = ctx.new_page()

        # ── Essex ──
        print('\n=== Scraping Essex County ===')
        try:
            essex_records = scrape_essex(page, from_str, to_str)
            print(f'  Essex raw records: {len(essex_records)}')
            all_records.extend(essex_records)
        except Exception as e:
            print(f'  Essex scrape error: {e}')

        # ── Union ──
        print('\n=== Scraping Union County ===')
        try:
            union_records = scrape_union(page, from_str, to_str)
            print(f'  Union raw records: {len(union_records)}')
            all_records.extend(union_records)
        except Exception as e:
            print(f'  Union scrape error: {e}')

        # ── Passaic ──
        print('\n=== Scraping Passaic County ===')
        try:
            passaic_records = scrape_passaic(page, from_str, to_str)
            print(f'  Passaic raw records: {len(passaic_records)}')
            all_records.extend(passaic_records)
        except Exception as e:
            print(f'  Passaic scrape error: {e}')

        browser.close()

    # Deduplicate by instrument number
    seen_instruments = set()
    unique_records = []
    for r in all_records:
        key = r.get('instrument_number') or f"{r['county']}-{r['name']}-{r['date']}"
        if key not in seen_instruments:
            seen_instruments.add(key)
            unique_records.append(r)

    print(f'\nTotal unique records: {len(unique_records)}')

    # Enrich with addresses
    print('\nEnriching addresses via NJ MOD-IV...')
    for rec in unique_records:
        enrich_address(rec)

    # Compute scores
    for rec in unique_records:
        try:
            filed_date = datetime.strptime(rec['date'], '%m/%d/%Y')
            days_old   = (datetime.now() - filed_date).days
        except Exception:
            days_old = 0

        rec['days_old'] = days_old
        rec['score']    = kps_score(0, 0, days_old, rec.get('city', ''))

    return unique_records


# ─────────────────────────────────────────────────────────────────────────────
#  Main loop
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print(f'\nNJ Foreclosure Bot v2 — {datetime.now()}')
    print(f'TEST_MODE={TEST_MODE} | LOOKBACK_DAYS={LOOKBACK_DAYS}')
    print('Source: Essex/Union/Passaic County PRESS portals (direct)')
    print('─' * 60)

    seen = set() if TEST_MODE else load_seen()
    print(f'Seen cache: {len(seen)} instruments\n')

    records = scrape_all()
    print(f'\nScraped {len(records)} total records')

    new_records = [r for r in records if
                   (r.get('instrument_number') or
                    f"{r['county']}-{r['name']}-{r['date']}") not in seen]
    print(f'New records to process: {len(new_records)}')

    if not new_records:
        print('Nothing new to process.')
        return

    for prop in new_records:
        key   = prop.get('instrument_number') or f"{prop['county']}-{prop['name']}-{prop['date']}"
        score = prop['score']

        print(f'\nProcessing: {prop["name"]} | {prop.get("address","(no addr)")} '
              f'{prop["city"]}, NJ {prop.get("zip","")} | '
              f'{prop["county"]} | Filed: {prop["date"]} | Score: {score}')

        if score < 25:
            print(f'  SKIP - score {score} below minimum threshold')
            seen.add(key)
            continue

        if TEST_MODE:
            print(f'  TEST MODE - would create lead + skip trace + enroll drip')
            print(f'  Full record: {json.dumps(prop, indent=2)}')
            seen.add(key)
            continue

        lead_id = create_lead(prop)
        if not lead_id:
            print(f'  Failed to create lead - skipping')
            continue

        skip_trace(lead_id)
        enroll_drip(lead_id, score)
        seen.add(key)

    if not TEST_MODE:
        save_seen(seen)

    print(f'\nDone. Processed {len(new_records)} records.')


if __name__ == '__main__':
    if TEST_MODE:
        try:
            main()
        except Exception as e:
            print(f'Error in test run: {e}')
            import traceback; traceback.print_exc()
            import sys; sys.exit(0)
    else:
        while True:
            try:
                main()
            except Exception as e:
                print(f'Error in main loop: {e}')
                import traceback; traceback.print_exc()
            print('\nSleeping 24 hours...')
            time.sleep(86400)
