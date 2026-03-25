import os
import re
import json
import time
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

NJ_USER        = os.environ['NJ_USERNAME']
NJ_PASS        = os.environ['NJ_PASSWORD']
RESIMPLI_TOKEN = os.environ['RESIMPLI_TOKEN']
SEEN_FILE      = 'seen_properties.json'
TEST_MODE      = os.environ.get('TEST_MODE', 'false').lower() == 'true'
LOOKBACK_DAYS  = int(os.environ.get('LOOKBACK_DAYS', '7'))

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
    if tax: pts += 8
    if absentee: pts += 5
    if vacant: pts += 2
    return min(pts, 25)


def market_score(city):
    city = city.lower()
    tier1 = {'newark', 'paterson', 'elizabeth'}
    tier2 = {'irvington', 'east orange', 'belleville'}
    if city in tier1: return 20
    if city in tier2: return 15
    return 10


def deal_score(prop):
    pts = 0
    if prop.get('phone'): pts += 5
    pt = prop.get('property_type', '')
    if 'sfr' in pt.lower() or 'single' in pt.lower(): pts += 5
    elif '2' in pt or '3' in pt or '4' in pt: pts += 4
    yr = prop.get('year_built', 0)
    if yr and yr < 1980: pts += 3
    if not prop.get('bankruptcy'): pts += 3
    return min(pts, 20)


def kps(prop):
    ev = prop.get('estimated_value', 0) or 0
    lb = prop.get('loan_balance', 0) or 0
    days = prop.get('days_since_filing', 999)
    eq = equity_score(ev, lb)
    di = distress_score(days, prop.get('tax_delinquent', False),
                        prop.get('absentee', False), prop.get('vacant', False))
    mk = market_score(prop.get('city', ''))
    dl = deal_score(prop)
    return eq + di + mk + dl


def assign_drip(score):
    if score >= 80: return DRIP_HOT
    if score >= 65: return DRIP_WARM
    if score >= 45: return DRIP_COLD
    return None


def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()


def save_seen(seen):
    with open(SEEN_FILE, 'w') as f:
        json.dump(list(seen), f)


def nj_login():
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    r = session.get('https://www.njlispendens.com/member/login')
    soup = BeautifulSoup(r.text, 'html.parser')
    attempt = ''
    inp = soup.find('input', {'name': 'login_attempt_id'})
    if inp:
        attempt = inp.get('value', '')
    data = {
        'amember_login': NJ_USER,
        'amember_pass': NJ_PASS,
        'login_attempt_id': attempt,
        'amember_redirect_url': '/member/property'
    }
    r2 = session.post('https://www.njlispendens.com/member/login', data=data, allow_redirects=True)
    if 'logout' in r2.text.lower():
        print('Logged in to NJLisPendens')
    else:
        print(f'WARNING: Login may have failed - no logout link found')
        print(f'Login status: {r2.status_code} | URL: {r2.url}')
    return session


def parse_card(card):
    text = card.get_text(separator=' ', strip=True)
    date_m = re.search(r'File Date[:\s]+([\d\-/]+)', text)
    docket_m = re.search(r'Docket No[:\s]+(\S+)', text)
    county_m = re.search(r'County[:\s]+([^\n\t]+)', text)
    city_m = re.search(r'([A-Za-z][A-Za-z\s]+)\s+NJ\s+\d{5}', text)
    zip_m = re.search(r'NJ\s+(\d{5})', text)
    mort_m = re.search(r'Orig Mortgage[:\s]+([\d,]+)', text)
    date_str = date_m.group(1).strip() if date_m else ''
    docket = docket_m.group(1).strip() if docket_m else ''
    county = county_m.group(1).strip() if county_m else ''
    city = city_m.group(1).strip() if city_m else ''
    zip_code = zip_m.group(1).strip() if zip_m else ''
    loan_balance = 0
    if mort_m:
        try:
            loan_balance = int(mort_m.group(1).replace(',', ''))
        except Exception:
            pass
    try:
        if '-' in date_str:
            filed_date = datetime.strptime(date_str, '%Y-%m-%d')
        else:
            filed_date = datetime.strptime(date_str, '%m/%d/%Y')
    except Exception:
        filed_date = datetime.now()
    days_since = (datetime.now() - filed_date).days
    return {
        'docket': docket,
        'address': city + ' NJ ' + zip_code,
        'city': city,
        'zip': zip_code,
        'county': county,
        'date_filed': date_str,
        'days_since_filing': days_since,
        'filed_date': filed_date,
        'estimated_value': 0,
        'loan_balance': loan_balance,
        'phone': None,
        'property_type': 'SFR',
        'year_built': 0,
        'bankruptcy': False,
        'tax_delinquent': False,
        'absentee': False,
        'vacant': False
    }


def scrape_properties(session):
    props = []
    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)
    page = 0
    while True:
        url = f'https://www.njlispendens.com/member/property?per_page=50&cp={page}'
        r = session.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        cards = soup.select('.pop-row')
        if not cards:
            print(f'No cards on page {page} - stopping pagination')
            break
        found_any = False
        for card in cards:
            try:
                prop = parse_card(card)
                county = prop.get('county', '')
                filed_date = prop.get('filed_date', datetime.min)
                if county not in TARGET_COUNTIES:
                    continue
                if filed_date < cutoff:
                    continue
                found_any = True
                props.append(prop)
                print(f'  Found: {prop["docket"]} {prop["city"]} ({prop["county"]}) {prop["date_filed"]}')
            except Exception as e:
                print(f'Error parsing card: {e}')
        if not found_any:
            print(f'No matching cards on page {page} - stopping')
            break
        page += 1
        time.sleep(1)
    return props


def create_lead(prop):
    payload = {
        'firstName': '',
        'lastName': '',
        'propertyAddress': prop.get('address', ''),
        'propertyCity': prop.get('city', ''),
        'propertyState': 'NJ',
        'propertyZip': prop.get('zip', ''),
        'county': prop.get('county', ''),
        'leadSource': 'NJLisPendens',
        'notes': f"Filed: {prop.get('date_filed','')} | Docket: {prop.get('docket','')}"
    }
    r = requests.post(f'{RESIMPLI_BASE}/lead/save',
                      headers=RESIMPLI_HEADERS, json=payload)
    if r.status_code == 200:
        data = r.json()
        lead_id = data.get('data', {}).get('_id') or data.get('_id')
        print(f'Created lead {lead_id} for {prop.get("address")}')
        return lead_id
    else:
        print(f'Lead create failed {r.status_code}: {r.text[:200]}')
        return None


def skip_trace(lead_id, prop):
    payload = {
        'leadId': lead_id,
        'address': prop.get('address', ''),
        'city': prop.get('city', ''),
        'state': 'NJ',
        'zip': prop.get('zip', '')
    }
    r = requests.post(f'{RESIMPLI_BASE}/lead/leadSkipTrace',
                      headers=RESIMPLI_HEADERS, json=payload)
    print(f'Skip trace {r.status_code} for lead {lead_id}')


def enroll_drip(lead_id, drip_id):
    payload = {
        'leadId': lead_id,
        'masterDripId': drip_id
    }
    r = requests.post(f'{RESIMPLI_BASE}/masterDrip/assignToLead',
                      headers=RESIMPLI_HEADERS, json=payload)
    print(f'Drip enroll {r.status_code} for lead {lead_id} drip {drip_id}')


def process_prop(prop, seen):
    docket = prop.get('docket', '')
    if docket and docket in seen:
        return False
    score = kps(prop)
    print(f'KPS={score} for {prop.get("address")} ({prop.get("county")})')
    if score < 25:
        print(f'  Score too low, skipping')
        if docket:
            seen.add(docket)
        return False
    if TEST_MODE:
        print(f'  TEST_MODE: would create lead, skip trace, drip (score={score})')
        if docket:
            seen.add(docket)
        return True
    lead_id = create_lead(prop)
    if not lead_id:
        return False
    skip_trace(lead_id, prop)
    drip_id = assign_drip(score)
    if drip_id:
        enroll_drip(lead_id, drip_id)
    if docket:
        seen.add(docket)
    return True


def run():
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    print(f'NJ Foreclosure Bot - {now} | TEST_MODE={TEST_MODE} | LOOKBACK_DAYS={LOOKBACK_DAYS}')
    if TEST_MODE:
        print('TEST MODE: clearing seen_properties cache for fresh run')
        seen = set()
    else:
        seen = load_seen()
    print(f'Loaded {len(seen)} seen dockets')
    try:
        session = nj_login()
    except Exception as e:
        print(f'Login failed: {e}')
        return
    props = scrape_properties(session)
    print(f'Scraped {len(props)} properties in target counties')
    if not props:
        print('Nothing to process.')
        return
    new_count = 0
    for prop in props:
        try:
            if process_prop(prop, seen):
                new_count += 1
        except Exception as e:
            print(f'Error processing {prop.get("address")}: {e}')
    save_seen(seen)
    print(f'Done. Processed {new_count} new leads.')


if __name__ == '__main__':
    while True:
        run()
        if TEST_MODE:
            print('TEST_MODE: exiting after one run')
            break
        print('Sleeping 24h...')
        time.sleep(86400)
