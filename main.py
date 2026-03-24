import os
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
    'Authorization': 'Bearer ' + RESIMPLI_TOKEN,
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
    pts = 10 if days<=7 else 7 if days<=30 else 4 if days<=90 else 0
    if tax: pts += 8
    if absentee: pts += 5
    if vacant: pts += 2
    return min(pts, 25)


def market_score(city):
    c = city.lower()
    if any(x in c for x in ['newark','paterson','elizabeth']): return 20
    if any(x in c for x in ['irvington','east orange','belleville']): return 15
    if any(x in c for x in ['bloomfield','montclair','nutley','clifton','passaic','linden','plainfield','kearny','harrison']): return 10
    return 5


def deal_score(phone, mail, ptype, yr, bk=False):
    pts = 0
    if phone: pts += 5
    if mail: pts += 4
    pt = (ptype or '').lower()
    if 'single' in pt or 'sfr' in pt or '1 family' in pt: pts += 5
    elif any(x in pt for x in ['2','3','4']): pts += 4
    if yr and yr < 1980: pts += 3
    if not bk: pts += 3
    return min(pts, 20)


def calculate_kps(p):
    total = (equity_score(p.get('ev',0), p.get('lb',0)) +
             distress_score(p.get('days',999), p.get('tax',False), p.get('absentee',False), p.get('vacant',False)) +
             market_score(p.get('city','')) +
             deal_score(p.get('phone_found',False), p.get('mail_found',False), p.get('ptype',''), p.get('yr'), p.get('bk',False)))
    label = 'HOT' if total>=80 else 'WARM' if total>=65 else 'FOLLOW UP' if total>=45 else 'MONITOR' if total>=25 else 'SKIP'
    return total, label


def get_drip_id(score):
    if score >= 80: return DRIP_HOT
    if score >= 65: return DRIP_WARM
    if score >= 45: return DRIP_COLD
    return None


def load_seen():
    if TEST_MODE:
        print('TEST MODE: clearing seen_properties cache for fresh run')
        return set()
    try:
        with open(SEEN_FILE) as f: return set(json.load(f))
    except: return set()


def save_seen(seen):
    if not TEST_MODE:
        with open(SEEN_FILE, 'w') as f: json.dump(list(seen), f)
    else:
        print('TEST MODE: not persisting seen cache')


def parse_card(card):
    """Parse a .pop-row card into a property dict."""
    obj = {}
    for trm in card.select('.mb_tx_trm'):
        for el in trm.select('.mb_tc1, .mb_tc2, .mb_tc'):
            text = el.get_text(strip=True)
            if ':' in text:
                colon = text.index(':')
                key = text[:colon].strip()
                val = text[colon+1:].strip()
                if key and val:
                    obj[key] = val
            elif text and 'Address' not in obj.get('_raw',''):
                # bare text after Address label = street
                if 'Address' in str(obj):
                    obj['Street'] = text
    return obj


def scrape_page(session, page, date_added_val):
    """Scrape one page of results."""
    params = {
        'Interest_Rate': '',
        'Plaintiff': '',
        'mortgage_from': '',
        'mortgage_to': '',
        'Monthly_Payment_from': '',
        'Monthly_Payment_to': '',
        'date_added': date_added_val,
        'dr_entry_month_from': '', 'dr_entry_day_from': '', 'dr_entry_year_from': '',
        'dr_entry_month_to': '',   'dr_entry_day_to': '',   'dr_entry_year_to': '',
        'entry_month': '', 'entry_day': '', 'entry_year': '',
        'mort_entry_month_from': '', 'mort_entry_day_from': '', 'mort_entry_year_from': '',
        'mort_entry_month_to': '',   'mort_entry_day_to': '',   'mort_entry_year_to': '',
        'Address': '',
        'City': '',
        'Zip_Code': '',
        'Attorney': '',
        'Docket_Number': '',
        'per_page': '50',
        'search': 'Search',
        'cp': str(page)
    }
    r = session.get('https://www.njlispendens.com/member/property', params=params, timeout=30)
    return r.text


def scrape():
    s = requests.Session()
    # Login via aMember - must GET login page first to extract dynamic login_attempt_id
    login_page = s.get('https://www.njlispendens.com/member/property', timeout=30)
    soup_login = BeautifulSoup(login_page.text, 'html.parser')
    attempt_input = soup_login.find('input', {'name': 'login_attempt_id'})
    login_attempt_id = attempt_input['value'] if attempt_input else '1'
    login_resp = s.post(
        'https://www.njlispendens.com/member/login',
        data={
            'amember_login': NJ_USER,
            'amember_pass': NJ_PASS,
            'login_attempt_id': login_attempt_id,
            'amember_redirect_url': '/member/property'
        },
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        allow_redirects=True,
        timeout=30
    )
    print('Login status: ' + str(login_resp.status_code) + ' | URL: ' + login_resp.url)
    # Verify we're logged in by checking for logout link
    if 'Logout' not in login_resp.text and 'kfreedman' not in login_resp.text.lower():
        print('WARNING: Login may have failed - no logout link found')

    # Use date_added=30 (max available = within 30 days); we filter by LOOKBACK_DAYS in code
    date_val = '30' if LOOKBACK_DAYS > 7 else '7'
    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)

    props = []
    page = 0
    seen_dockets = set()

    while True:
        html = scrape_page(s, page, date_val)
        soup = BeautifulSoup(html, 'html.parser')
        cards = soup.select('.pop-row')
        if not cards:
            print('No cards on page ' + str(page) + ' - stopping pagination')
            break

        new_on_page = 0
        for card in cards:
            try:
                # Extract fields from .mb_tc divs
                fields = {}
                for trm in card.select('.mb_tx_trm'):
                    tcs = trm.select('.mb_tc1, .mb_tc2, .mb_tc')
                    for tc in tcs:
                        t = tc.get_text(separator=' ', strip=True)
                        if ':' in t:
                            idx = t.index(':')
                            k = t[:idx].strip()
                            v = t[idx+1:].strip()
                            if k:
                                fields[k] = v

                docket    = fields.get('Docket No', '').strip()
                file_date = fields.get('File Date', '').strip()
                county    = fields.get('County', '').strip()
                defendant = fields.get('Defendant', '').strip()
                plaintiff = fields.get('Plaintiff', '').strip()
                lb_str    = fields.get('Orig Mortgage', '0').replace(',','').replace('$','').strip()
                address_block = fields.get('Address', '').strip()

                # Skip dupes within this scrape run
                if docket in seen_dockets:
                    continue
                seen_dockets.add(docket)

                # Filter to target counties only
                if county not in TARGET_COUNTIES:
                    continue

                # Parse file date and filter by lookback window
                try:
                    fd = datetime.strptime(file_date, '%Y-%m-%d')
                except:
                    fd = datetime.now()
                if fd < cutoff:
                    continue

                days_old = (datetime.now() - fd).days

                # Parse address block: "123 Main St\nNewark NJ 07102"
                addr_lines = [l.strip() for l in address_block.split('\n') if l.strip()]
                street = addr_lines[0] if addr_lines else ''
                city_state_zip = addr_lines[-1] if len(addr_lines) > 1 else ''
                # Parse "Newark NJ 07102"
                parts = city_state_zip.split()
                zip_code = parts[-1] if parts and parts[-1].isdigit() else ''
                state    = parts[-2] if len(parts) >= 2 else 'NJ'
                city     = ' '.join(parts[:-2]) if len(parts) > 2 else city_state_zip

                try:
                    lb = float(lb_str)
                except:
                    lb = 0

                props.append({
                    'docket':    docket,
                    'address':   street,
                    'city':      city,
                    'state':     state,
                    'zip':       zip_code,
                    'county':    county,
                    'plaintiff': plaintiff,
                    'owner':     defendant,
                    'lb':        lb,
                    'file_date': file_date,
                    'days':      days_old,
                    'ev': 0, 'phone': '', 'phone_found': False, 'mail_found': False,
                    'mailing_address': '', 'mailing_city': '', 'mailing_state': '', 'mailing_zip': '',
                    'ptype': '', 'yr': None, 'tax': False, 'absentee': False, 'vacant': False, 'bk': False
                })
                new_on_page += 1
            except Exception as e:
                print('Parse error: ' + str(e))

        print('Page ' + str(page) + ': ' + str(len(cards)) + ' cards, ' + str(new_on_page) + ' in target counties/range')

        # Stop if we've gone past our lookback window (cards are newest-first)
        # If all cards on page are too old, stop
        too_old = 0
        for card in cards:
            for trm in card.select('.mb_tx_trm'):
                for tc in trm.select('.mb_tc1'):
                    t = tc.get_text(strip=True)
                    if 'File Date' in t:
                        try:
                            fd_str = t.replace('File Date:','').strip()
                            fd = datetime.strptime(fd_str, '%Y-%m-%d')
                            if fd < cutoff:
                                too_old += 1
                        except: pass
        if too_old == len(cards):
            print('All cards on page are beyond lookback window - stopping')
            break

        # Stop if fewer than 50 results (last page)
        if len(cards) < 50:
            break

        page += 1
        time.sleep(1)

    return props


def skip_trace_resimpli(props):
    if not props: return props
    for p in props:
        try:
            parts = p['owner'].split(',')
            last  = parts[0].strip() if parts else ''
            first = parts[1].strip() if len(parts) > 1 else ''
            ap    = p['address'].split()
            house  = ap[0] if ap else ''
            street = ' '.join(ap[1:]) if len(ap) > 1 else p['address']
            payload = {
                'firstName': first, 'lastName': last,
                'address': house, 'street': street,
                'city': p['city'], 'state': 'NJ', 'zip': p['zip']
            }
            r = requests.post('https://live-api.resimpli.com/api/v4/skipTrace/singleSkipTrace',
                              json=payload, headers=RESIMPLI_HEADERS, timeout=30)
            if r.status_code in (200, 201):
                d = r.json().get('data', {})
                phones = d.get('phones', [])
                if phones:
                    p['phone_found'] = True
                    p['phone'] = phones[0].get('number', '')
                mail = d.get('mailingAddress', {})
                if mail.get('address'):
                    p['mail_found'] = True
                    p['mailing_address'] = mail.get('address', '')
                    p['mailing_city']    = mail.get('city', '')
                    p['mailing_state']   = mail.get('state', '')
                    p['mailing_zip']     = mail.get('zip', '')
                pi = d.get('propertyInfo', {})
                if pi:
                    p['ev']       = pi.get('estimatedValue', 0) or 0
                    p['ptype']    = pi.get('propertyUse', '')
                    p['yr']       = pi.get('yearBuilt')
                    p['absentee'] = pi.get('absenteeOwner', False)
                    p['vacant']   = pi.get('vacant', False)
            else:
                print('Skip trace HTTP ' + str(r.status_code) + ' for ' + p['address'] + ' | resp: ' + r.text[:200])
            time.sleep(0.5)
        except Exception as e:
            print('Skip trace error: ' + str(e))
    return props


def push_lead(p, score, label, drip_id):
    parts = p['owner'].split(',')
    last  = parts[0].strip() if parts else 'Owner'
    first = parts[1].strip() if len(parts) > 1 else 'Unknown'
    payload = {
        'firstName': first,
        'lastName':  last,
        'phone':     p.get('phone',''),
        'propertyAddress': p['address'],
        'propertyCity':    p['city'],
        'propertyState':   'NJ',
        'propertyZip':     p['zip'],
        'leadSource': 'NJ Pre-Foreclosure',
        'tags': [label, p.get('county','')],
        'mailingAddress': p.get('mailing_address',''),
        'mailingCity':    p.get('mailing_city',''),
        'mailingState':   p.get('mailing_state',''),
        'mailingZip':     p.get('mailing_zip',''),
        'notes': ('KPS: '+str(score)+' - '+label+'\n'
                  'County: '+p.get('county','')+'\n'
                  'Docket: '+p['docket']+'\n'
                  'Mortgage: $'+str(int(p['lb']))+'\n'
                  'Est Value: $'+str(int(p['ev']))+'\n'
                  'Filed: '+p['file_date']+'\n'
                  'Plaintiff: '+p['plaintiff']+'\n'
                  'Year Built: '+str(p['yr'])+'\n'
                  'Absentee: '+str(p['absentee']))
    }
    r = requests.post('https://live-api.resimpli.com/api/v4/lead/create',
                      json=payload, headers=RESIMPLI_HEADERS, timeout=30)
    if r.status_code not in (200,201):
        print('Lead failed: '+str(r.status_code)+' '+r.text[:200])
        return False
    lid = (r.json().get('data') or {}).get('_id') or r.json().get('_id')
    if not lid: return False
    print('Created: '+p['address']+', '+p['city']+' | KPS:'+str(score)+' '+label)
    if drip_id:
        dr = requests.post('https://live-api.resimpli.com/api/v4/drip/assignDripToLead',
                           json={'leadId':lid,'dripCampaignId':drip_id},
                           headers=RESIMPLI_HEADERS, timeout=30)
        print('  Drip: ' + ('OK' if dr.status_code in (200,201) else 'FAILED '+str(dr.status_code)))
    return True


def main():
    mode_str = ('TEST MODE ('+str(LOOKBACK_DAYS)+'-day lookback, no cache)'
                if TEST_MODE else str(LOOKBACK_DAYS)+'-day lookback')
    print('NJ Foreclosure Bot - ' + datetime.now().strftime('%Y-%m-%d %H:%M') + ' | ' + mode_str)
    seen = load_seen()
    props = scrape()
    new_props = [p for p in props if p['docket'] not in seen]
    print('Scraped '+str(len(props))+' in target counties, '+str(len(new_props))+' new')
    if not new_props:
        print('Nothing to process.')
        return
    print('Skip tracing ' + str(len(new_props)) + ' properties...')
    new_props = skip_trace_resimpli(new_props)
    pushed = 0
    for p in new_props:
        score, label = calculate_kps(p)
        if label == 'SKIP':
            print('Skipping '+p['address']+' (KPS '+str(score)+')')
            seen.add(p['docket'])
            continue
        if push_lead(p, score, label, get_drip_id(score)):
            seen.add(p['docket']); pushed += 1
        time.sleep(0.5)
    save_seen(seen)
    print('Done: '+str(pushed)+' leads imported')


if __name__ == '__main__':
    main()
