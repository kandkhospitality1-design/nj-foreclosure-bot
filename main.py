import os
import json
import time
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

NJ_USER         = os.environ['NJ_USERNAME']
NJ_PASS         = os.environ['NJ_PASSWORD']
RESIMPLI_TOKEN  = os.environ['RESIMPLI_TOKEN']
SEEN_FILE       = 'seen_properties.json'
TEST_MODE       = os.environ.get('TEST_MODE', 'false').lower() == 'true'
LOOKBACK_DAYS   = int(os.environ.get('LOOKBACK_DAYS', '7'))

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
        city = (city or '').lower()
    tier1 = {'newark', 'paterson', 'elizabeth'}
    tier2 = {'irvington', 'east orange', 'belleville'}
    if city in tier1:   return 20
            if city in tier2:   return 15
                    return 10


def deal_score(has_phone, sfr, built_year, no_bk):
        pts = 0
    if has_phone: pts += 5
            if sfr:       pts += 5
elif built_year and built_year <= 4: pts += 4
    if built_year and built_year <= 1980: pts += 3
            if no_bk:     pts += 3
                    return min(pts, 20)


def kps(prop):
        ev = prop.get('estimated_value', 0) or 0
    lb = prop.get('total_liens', 0) or 0
    days = prop.get('days_since_filed', 90)
    city = prop.get('city', '')
    has_phone = bool(prop.get('phone'))
    sfr = prop.get('property_type', '').lower() in ('sfr', 'single family', 'single-family')
    built = prop.get('year_built')
    no_bk = not prop.get('bankruptcy', False)

    score = (equity_score(ev, lb)
                          + distress_score(days)
                          + market_score(city)
                          + deal_score(has_phone, sfr, built, no_bk))
    return score


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
        if not TEST_MODE:
                    with open(SEEN_FILE, 'w') as f:
                                    json.dump(list(seen), f)


def nj_login():
        session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})

    login_page = session.get('https://www.njlispendens.com/member/login')
    soup = BeautifulSoup(login_page.text, 'html.parser')
    attempt_input = soup.find('input', {'name': 'login_attempt_id'})
    attempt_id = attempt_input['value'] if attempt_input else ''

    payload = {
                'amember_login': NJ_USER,
                'amember_pass': NJ_PASS,
                'login_attempt_id': attempt_id,
                'amember_redirect_url': '/member/property'
    }
    resp = session.post('https://www.njlispendens.com/member/login', data=payload)
    if 'member/property' not in resp.url and 'logout' not in resp.text.lower():
                raise Exception('NJLisPendens login failed')
    print('Logged in to NJLisPendens')
    return session


def scrape_properties(session):
        properties = []
    page = 0
    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)

    while True:
                params = {
                                'per_page': '50',
                                'search': 'Search',
                                'cp': str(page)
                }
        resp = session.get('https://www.njlispendens.com/member/property', params=params)
        soup = BeautifulSoup(resp.text, 'html.parser')
        cards = soup.select('.pop-row')

        if not cards:
                        print(f'No cards on page {page}, stopping')
                        break

        page_has_new = False
        for card in cards:
                        try:
                                            prop = parse_card(card)
                                            if not prop:
                                                                    continue

                                            if prop.get('county') not in TARGET_COUNTIES:
                                                                    continue

                                            filed_date = prop.get('filed_date')
                                            if filed_date and filed_date < cutoff:
                                                                    if not page_has_new:
                                                                                                print(f'Reached cutoff date on page {page}')
                                                                                                return properties
                                                                                            continue

                page_has_new = True
                properties.append(prop)
except Exception as e:
                print(f'Error parsing card: {e}')

        print(f'Page {page}: {len(cards)} cards, {len(properties)} props so far')
        page += 1
        time.sleep(1)

    return properties


def parse_card(card):
        prop = {}

    def text(sel):
                el = card.select_one(sel)
        return el.get_text(strip=True) if el else ''

    address_el = card.select_one('.pop-address, .address, h3, h4, .street')
    if address_el:
                prop['address'] = address_el.get_text(strip=True)

    all_text = card.get_text(' ', strip=True)

    for label in ['County:', 'COUNTY:']:
                if label in all_text:
                                idx = all_text.index(label) + len(label)
                                prop['county'] = all_text[idx:idx+30].split()[0].strip()
                                break

    for label in ['City:', 'CITY:']:
                if label in all_text:
                                idx = all_text.index(label) + len(label)
                                prop['city'] = all_text[idx:idx+30].split()[0].strip()
                                break

    for label in ['Docket:', 'Docket #:', 'DOCKET:']:
                if label in all_text:
                                idx = all_text.index(label) + len(label)
                                prop['docket'] = all_text[idx:idx+30].split()[0].strip()
                                break

    for label in ['Filed:', 'Date Filed:', 'DATE FILED:']:
                if label in all_text:
                                idx = all_text.index(label) + len(label)
                                date_str = all_text[idx:idx+15].strip().split()[0]
                                try:
                                                    filed_date = datetime.strptime(date_str, '%m/%d/%Y')
                                                    prop['filed_date'] = filed_date
                                                    prop['days_since_filed'] = (datetime.now() - filed_date).days
except Exception:
                pass
            break

    for label in ['Zip:', 'ZIP:']:
                if label in all_text:
                                idx = all_text.index(label) + len(label)
                                prop['zip'] = all_text[idx:idx+10].strip().split()[0]
                                break

    if not prop.get('docket') or not prop.get('county'):
                return None

    return prop


def skip_trace(lead_id, prop):
        body = {
                    'leadId': lead_id,
                    'city': prop.get('city', ''),
                    'state': 'NJ',
                    'address': prop.get('address', ''),
                    'zip': prop.get('zip', '')
        }
    resp = requests.post(
                f'{RESIMPLI_BASE}/lead/leadSkipTrace',
                headers=RESIMPLI_HEADERS,
                json=body,
                timeout=30
    )
    data = resp.json()
    if data.get('statusCode') == 200:
                print(f'Skip traced lead {lead_id}')
        return data.get('data', {})
else:
        print(f'Skip trace failed: {data.get("message")}')
        return {}


def create_lead(prop):
        body = {
                    'streetAddress': prop.get('address', ''),
                    'city': prop.get('city', ''),
                    'state': 'NJ',
                    'zipCode': prop.get('zip', ''),
                    'county': prop.get('county', ''),
                    'source': 'NJLisPendens',
                    'docketNumber': prop.get('docket', ''),
                    'notes': f"Foreclosure filing. Docket: {prop.get('docket')}. Filed: {prop.get('filed_date', 'unknown')}"
        }
    resp = requests.post(
                f'{RESIMPLI_BASE}/lead/save',
                headers=RESIMPLI_HEADERS,
                json=body,
                timeout=30
    )
    data = resp.json()
    if data.get('statusCode') == 200:
                lead_id = data.get('data', {}).get('_id') or data.get('data', {}).get('id')
        print(f'Created lead {lead_id} for {prop.get("address")}')
        return lead_id
else:
        print(f'Lead create failed: {data.get("message")} | body: {json.dumps(body)[:200]}')
        return None


def enroll_drip(lead_id, drip_id):
        body = {'leadId': lead_id, 'masterDripId': drip_id}
    resp = requests.post(
                f'{RESIMPLI_BASE}/masterDrip/assignToLead',
                headers=RESIMPLI_HEADERS,
                json=body,
                timeout=30
    )
    data = resp.json()
    msg = data.get('message', '')
    status = data.get('statusCode')
    print(f'Drip enroll status={status} msg={msg}')
    return status == 200


def process_property(prop, seen):
        docket = prop.get('docket')
    if docket in seen:
                return False

    score = kps(prop)
    print(f'KPS={score} for {prop.get("address")} ({prop.get("county")})')

    if score < 25:
                print('Score too low, skipping')
        seen.add(docket)
        return False

    if TEST_MODE:
                print(f'TEST_MODE: would create lead + drip for score {score}')
        seen.add(docket)
        return True

    lead_id = create_lead(prop)
    if not lead_id:
                return False

    seen.add(docket)

    skip_trace(lead_id, prop)

    drip_id = assign_drip(score)
    if drip_id:
                enroll_drip(lead_id, drip_id)

    return True


def run():
        print(f'Starting bot | TEST_MODE={TEST_MODE} | LOOKBACK_DAYS={LOOKBACK_DAYS}')
    seen = load_seen()
    print(f'Loaded {len(seen)} seen dockets')

    try:
                session = nj_login()
except Exception as e:
        print(f'Login failed: {e}')
        return

    props = scrape_properties(session)
    print(f'Scraped {len(props)} properties in target counties within {LOOKBACK_DAYS} days')

    new_count = 0
    for prop in props:
                try:
                                if process_property(prop, seen):
                                                    new_count += 1
                                                time.sleep(2)
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
