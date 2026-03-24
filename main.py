import os
import json
import time
import requests
from datetime import datetime
from bs4 import BeautifulSoup

NJ_USER        = os.environ['NJ_USERNAME']
NJ_PASS        = os.environ['NJ_PASSWORD']
RESIMPLI_TOKEN = os.environ['RESIMPLI_TOKEN']
SEEN_FILE      = 'seen_properties.json'

DRIP_HOT  = '69c2f816a121be22d81a6c85'
DRIP_WARM = '69c2f97068818e6c3fa39cd5'
DRIP_COLD = '69c30c665ea684747e339fa3'

RESIMPLI_HEADERS = {
    'Authorization': 'Bearer ' + RESIMPLI_TOKEN,
    'Content-Type': 'application/json'
}

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
    try:
        with open(SEEN_FILE) as f: return set(json.load(f))
    except: return set()

def save_seen(seen):
    with open(SEEN_FILE, 'w') as f: json.dump(list(seen), f)

def scrape():
    s = requests.Session()
    s.post('https://www.njlispendens.com/login',
           data={'username': NJ_USER, 'password': NJ_PASS},
           headers={'Content-Type': 'application/x-www-form-urlencoded'})
    r = s.get('https://www.njlispendens.com/member/property',
              params={'County[0]':'0','date_added':'7','per_page':'50','search':'Search','cp':'0'})
    soup = BeautifulSoup(r.text, 'html.parser')
    props = []
    for row in soup.select('table tbody tr'):
        cols = row.find_all('td')
        if len(cols) < 5: continue
        try:
            def txt(i): return cols[i].get_text(strip=True) if len(cols)>i else ''
            def dol(i):
                try: return float(txt(i).replace('$','').replace(',',''))
                except: return 0
            def days_since(ds):
                try: return (datetime.now()-datetime.strptime(ds.strip(),'%m/%d/%Y')).days
                except: return 999
            props.append({'docket':txt(0),'address':txt(1),'city':txt(2),'zip':txt(3),
                          'plaintiff':txt(4),'lb':dol(5),'file_date':txt(6),'owner':txt(7),
                          'ev':0,'phone':'','phone_found':False,'mail_found':False,
                          'mailing_address':'','mailing_city':'','mailing_state':'','mailing_zip':'',
                          'ptype':'','yr':None,'tax':False,'absentee':False,'vacant':False,'bk':False,
                          'days':days_since(txt(6))})
        except Exception as e:
            print('parse error: ' + str(e))
    return props

def skip_trace_resimpli(props):
    """Use REsimpli built-in skip tracing API"""
    if not props: return props
    for p in props:
        try:
            parts = p['owner'].split()
            first = parts[0] if parts else ''
            last = parts[-1] if len(parts) > 1 else ''
            ap = p['address'].split()
            house = ap[0] if ap else ''
            street = ' '.join(ap[1:]) if len(ap) > 1 else p['address']
            payload = {
                'firstName': first,
                'lastName': last,
                'address': house,
                'street': street,
                'city': p['city'],
                'state': 'NJ',
                'zip': p['zip']
            }
            r = requests.post('https://api.resimpli.com/skip-trace',
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
                    p['mailing_city'] = mail.get('city', '')
                    p['mailing_state'] = mail.get('state', '')
                    p['mailing_zip'] = mail.get('zip', '')
                pi = d.get('propertyInfo', {})
                if pi:
                    p['ev'] = pi.get('estimatedValue', 0) or 0
                    p['ptype'] = pi.get('propertyUse', '')
                    p['yr'] = pi.get('yearBuilt')
                    p['absentee'] = pi.get('absenteeOwner', False)
                    p['vacant'] = pi.get('vacant', False)
            time.sleep(0.5)
        except Exception as e:
            print('Skip trace error: ' + str(e))
    return props

def push_lead(p, score, label, drip_id):
    parts = p['owner'].split()
    payload = {
        'firstName': parts[0] if parts else 'Unknown',
        'lastName': parts[-1] if len(parts)>1 else 'Owner',
        'phone': p.get('phone',''),
        'propertyAddress': p['address'], 'propertyCity': p['city'],
        'propertyState': 'NJ', 'propertyZip': p['zip'],
        'leadSource': 'NJ Pre-Foreclosure', 'tags': [label],
        'mailingAddress':p.get('mailing_address',''), 'mailingCity':p.get('mailing_city',''),
        'mailingState':p.get('mailing_state',''), 'mailingZip':p.get('mailing_zip',''),
        'notes': 'KPS: '+str(score)+' - '+label+'\nDocket: '+p['docket']+'\nMortgage: $'+str(int(p['lb']))+'\nEst Value: $'+str(int(p['ev']))+'\nFiled: '+p['file_date']+'\nPlaintiff: '+p['plaintiff']+'\nYear Built: '+str(p['yr'])+'\nTax Delinquent: '+str(p['tax'])+'\nAbsentee: '+str(p['absentee'])
    }
    r = requests.post('https://api.resimpli.com/lead', json=payload, headers=RESIMPLI_HEADERS, timeout=30)
    if r.status_code not in (200,201):
        print('Lead failed: '+str(r.status_code)+' '+r.text[:200])
        return False
    lid = (r.json().get('data') or {}).get('_id') or r.json().get('_id')
    if not lid: return False
    print('Created: '+p['address']+' | KPS:'+str(score)+' '+label)
    if drip_id:
        dr = requests.post('https://api.resimpli.com/lead/'+lid+'/drip-campaign',
                           json={'dripCampaignId':drip_id}, headers=RESIMPLI_HEADERS, timeout=30)
        print('  Drip: ' + ('OK' if dr.status_code in (200,201) else 'FAILED '+str(dr.status_code)))
    return True

def main():
    print('NJ Foreclosure Bot - ' + datetime.now().strftime('%Y-%m-%d %H:%M'))
    seen = load_seen()
    props = scrape()
    new_props = [p for p in props if p['docket'] not in seen]
    print('Scraped '+str(len(props))+', '+str(len(new_props))+' new')
    if not new_props: return
    print('Skip tracing...')
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
