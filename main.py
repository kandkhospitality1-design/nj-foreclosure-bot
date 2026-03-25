import os
import re
import json
import time
import requests
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

NJ_USER        = os.environ['NJ_USERNAME']
NJ_PASS        = os.environ['NJ_PASSWORD']
RESIMPLI_TOKEN = os.environ['RESIMPLI_TOKEN']
SEEN_FILE      = 'seen_properties.json'
TEST_MODE      = os.environ.get('TEST_MODE', 'false').lower() == 'true'
LOOKBACK_DAYS  = int(os.environ.get('LOOKBACK_DAYS', '15'))

DRIP_HOT  = '69c2f816a121be22d81a6c85'
DRIP_WARM = '69c2f97068818e6c3fa39cd5'
DRIP_COLD = '69c30c665ea684747e339fa3'

TARGET_COUNTIES = {'Essex', 'Passaic', 'Union'}

RESIMPLI_HEADERS = {
    'Authorization': RESIMPLI_TOKEN,
    'Content-Type': 'application/json'
}
RESIMPLI_BASE = 'https://live-api.resimpli.com/api/v4'

def equity_score(ev, lb):
    if not ev:
        return 0
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
    hot  = {'newark', 'paterson', 'elizabeth'}
    warm = {'irvington', 'east orange', 'belleville'}
    c = city.lower().strip()
    if c in hot:  return 20
    if c in warm: return 15
    return 10

def deal_score(has_phone=False, prop_type='SFR', year_built=None, no_bankruptcy=True):
    pts = 0
    if has_phone: pts += 5
    if prop_type == 'SFR': pts += 5
    elif prop_type in ('2-4', 'duplex', 'triplex'): pts += 4
    if year_built and year_built < 1980: pts += 3
    if no_bankruptcy: pts += 3
    return min(pts, 20)

def kps_score(ev, lb, days, city, **kwargs):
    return (equity_score(ev, lb) + distress_score(days, **kwargs)
            + market_score(city) + deal_score())

def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()

def save_seen(seen):
    with open(SEEN_FILE, 'w') as f:
        json.dump(list(seen), f)

def create_lead(prop):
    payload = {
        'firstName': 'NJ Foreclosure',
        'lastName': prop.get('docket', 'Unknown'),
        'address': prop.get('city', ''),
        'state': 'NJ',
        'zip': prop.get('zip', ''),
        'leadSource': 'NJLisPendens',
        'notes': (
            f"County: {prop.get('county')} | Docket: {prop.get('docket')} | "
            f"Filed: {prop.get('date')} | Mortgage: {prop.get('mortgage')} | "
            f"Score: {prop.get('score')}"
        )
    }
    r = requests.post(f'{RESIMPLI_BASE}/lead/save', headers=RESIMPLI_HEADERS, json=payload)
    if r.status_code == 200:
        return r.json().get('data', {}).get('_id') or r.json().get('_id')
    print(f'Lead create failed: {r.status_code} {r.text[:200]}')
    return None

def skip_trace(lead_id):
    r = requests.post(
        f'{RESIMPLI_BASE}/lead/leadSkipTrace',
        headers=RESIMPLI_HEADERS,
        json={'leadId': lead_id}
    )
    if r.status_code != 200:
        print(f'Skip trace failed: {r.status_code} {r.text[:200]}')

def enroll_drip(lead_id, score):
    if score >= 80:
        drip_id = DRIP_HOT
    elif score >= 65:
        drip_id = DRIP_WARM
    elif score >= 45:
        drip_id = DRIP_COLD
    else:
        return
    r = requests.post(
        f'{RESIMPLI_BASE}/masterDrip/assignToLead',
        headers=RESIMPLI_HEADERS,
        json={'leadId': lead_id, 'masterDripId': drip_id}
    )
    if r.status_code != 200:
        print(f'Drip enroll failed: {r.status_code} {r.text[:200]}')

def do_login(page):
    """Attempt login, return True on success."""
    print('Navigating to login page...')
    page.goto('https://www.njlispendens.com/member/login', wait_until='domcontentloaded')
    page.wait_for_timeout(2000)

    # Fill credentials
    page.fill('input[name="amember_login"]', NJ_USER)
    page.wait_for_timeout(500)
    page.fill('input[name="amember_pass"]', NJ_PASS)
    page.wait_for_timeout(500)

    # Submit
    page.press('input[name="amember_pass"]', 'Enter')
    try:
        page.wait_for_url(
            lambda url: 'member/login' not in url,
            timeout=10000
        )
    except Exception:
        pass
    page.wait_for_timeout(3000)

    current_url = page.url
    page_text = page.content().lower()
    print(f'After login attempt - URL: {current_url}')

    # Success: redirected away from login page
    if 'member/login' not in current_url:
        print('Login successful (URL redirect confirmed)')
        return True

    # Fallback: try clicking the submit button
    print('URL still on login - trying submit button click...')
    try:
        page.click('input[type="submit"]')
        try:
            page.wait_for_url(
                lambda url: 'member/login' not in url,
                timeout=10000
            )
        except Exception:
            pass
        page.wait_for_timeout(3000)
        current_url = page.url
        print(f'After submit click - URL: {current_url}')
        if 'member/login' not in current_url:
            print('Login successful (submit click confirmed)')
            return True
    except Exception as e:
        print(f'Submit click failed: {e}')

    # Last-resort: check page content for logged-in indicators
    page_text = page.content().lower()
    if 'logout' in page_text or 'my account' in page_text or 'member/profile' in page_text:
        print('Login successful (content indicator found)')
        return True

    print(f'WARNING: login failed. Final URL: {page.url}')
    # Print first 500 chars of body for debugging
    print(f'Page snippet: {page.inner_text("body")[:500]}')
    return False

def scrape_with_playwright():
    properties = []
    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)

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

        if not do_login(page):
            browser.close()
            return properties

        print('Logged in to NJLisPendens successfully')

        page_num = 0
        while True:
            url = f'https://www.njlispendens.com/member/property?per_page=50&cp={page_num}'
            page.goto(url, wait_until='domcontentloaded')
            page.wait_for_timeout(2000)

            # Re-check login in case session expired mid-run
            if 'member/login' in page.url:
                print('Session expired mid-run, re-logging in...')
                if not do_login(page):
                    print('Re-login failed, stopping scrape')
                    break
                page.goto(url, wait_until='domcontentloaded')
                page.wait_for_timeout(2000)

            cards = page.query_selector_all('.pop-row')
            print(f'Page {page_num}: found {len(cards)} cards')

            if not cards:
                # Try alternate selectors
                cards = page.query_selector_all('.property-row, .listing-row, tr.data-row')
                print(f'Page {page_num}: alternate selector found {len(cards)} cards')

            if not cards:
                print(f'No cards on page {page_num} - stopping')
                break

            stop_early = False
            for card in cards:
                try:
                    text = card.inner_text()

                    # Extract county
                    county_m = re.search(r'County:\s*([A-Za-z]+)', text)
                    county = county_m.group(1).strip() if county_m else ''
                    if county not in TARGET_COUNTIES:
                        continue

                    # Extract file date
                    date_m = re.search(r'File Date:\s*(\d{1,2}/\d{1,2}/\d{4})', text)
                    if not date_m:
                        continue
                    file_date = datetime.strptime(date_m.group(1), '%m/%d/%Y')
                    if file_date < cutoff:
                        stop_early = True
                        continue
                    days_old = (datetime.now() - file_date).days

                    # Extract docket
                    docket_m = re.search(r'Docket(?:\s*No\.?)?:\s*([A-Z0-9\-]+)', text, re.I)
                    docket = docket_m.group(1).strip() if docket_m else ''

                    # Extract city + zip
                    city_m = re.search(r'([A-Za-z\s]+),\s*NJ\s*(\d{5})', text)
                    city = city_m.group(1).strip() if city_m else ''
                    zip_code = city_m.group(2) if city_m else ''

                    # Extract mortgage amount
                    mort_m = re.search(r'Orig(?:inal)?\s+Mortgage:\s*\$?([\d,\.]+)', text, re.I)
                    mortgage = mort_m.group(1).replace(',', '') if mort_m else '0'

                    score = kps_score(0, float(mortgage) if mortgage else 0, days_old, city)

                    properties.append({
                        'county': county,
                        'date': date_m.group(1),
                        'docket': docket,
                        'city': city,
                        'zip': zip_code,
                        'mortgage': mortgage,
                        'score': score,
                        'days_old': days_old,
                    })
                    print(f'  Found: {docket} | {city} ({county}) | Score: {score} | Filed: {date_m.group(1)}')
                except Exception as e:
                    print(f'  Card parse error: {e}')

            if stop_early:
                print('Reached properties older than cutoff - stopping pagination')
                break
            page_num += 1

        browser.close()
    return properties

def main():
    print(f'NJ Foreclosure Bot - {datetime.now()} | TEST_MODE={TEST_MODE} | LOOKBACK_DAYS={LOOKBACK_DAYS}')

    if TEST_MODE:
        print('TEST MODE: clearing seen_properties cache for fresh run')
        seen = set()
    else:
        seen = load_seen()
        print(f'Loaded {len(seen)} seen dockets')

    props = scrape_with_playwright()
    print(f'Scraped {len(props)} properties in target counties')

    new_props = [p for p in props if p['docket'] not in seen]
    print(f'New properties to process: {len(new_props)}')

    if not new_props:
        print('Nothing to process.')
    else:
        for prop in new_props:
            score = prop['score']
            docket = prop['docket']
            print(f'Processing {docket} | Score: {score} | {prop["city"]} ({prop["county"]})')

            if score < 25:
                print(f'  SKIP (score {score} < 25)')
                seen.add(docket)
                continue

            if TEST_MODE:
                print(f'  TEST MODE: would create lead, skip trace, enroll drip')
                print(f'  Score breakdown: {prop}')
                seen.add(docket)
                continue

            lead_id = create_lead(prop)
            if not lead_id:
                print(f'  Failed to create lead for {docket}')
                continue

            skip_trace(lead_id)
            enroll_drip(lead_id, score)
            seen.add(docket)
            print(f'  Done - lead {lead_id} created, skip traced, drip enrolled')

    if not TEST_MODE:
        save_seen(seen)

    if TEST_MODE:
        print('TEST_MODE: exiting after one run')
        return

    print('Sleeping 24 hours...')
    time.sleep(86400)

if __name__ == '__main__':
    if TEST_MODE:
        try:
            main()
        except Exception as e:
            print(f'Error in test run: {e}')
        import sys
        sys.exit(0)
    else:
        while True:
            try:
                main()
            except Exception as e:
                print(f'Error in main loop: {e}')
                time.sleep(300)
