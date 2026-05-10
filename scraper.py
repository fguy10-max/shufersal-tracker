import requests
import gzip
import json
import re
import os
import base64
from datetime import datetime
from collections import defaultdict
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io

# ── GitHub ───────────────────────────────────────────────
GH_TOKEN = os.environ.get('GH_TOKEN')
GH_REPO  = 'fguy10-max/shufersal-tracker'
GH_API   = 'https://api.github.com'

def github_get_sha(filename):
    """מחזיר את ה-SHA של קובץ קיים ב-GitHub (נדרש לעדכון)"""
    url = f'{GH_API}/repos/{GH_REPO}/contents/{filename}'
    r = requests.get(url, headers={'Authorization': f'token {GH_TOKEN}'})
    if r.status_code == 200:
        return r.json()['sha']
    return None

def github_upload(filename, data):
    """מעלה קובץ JSON ל-GitHub Repository"""
    content = json.dumps(data, ensure_ascii=False, separators=(',',':')).encode('utf-8')
    encoded = base64.b64encode(content).decode('utf-8')
    sha = github_get_sha(filename)
    url = f'{GH_API}/repos/{GH_REPO}/contents/{filename}'
    payload = {
        'message': f'עדכון מחירים {datetime.now().strftime("%Y-%m-%d")}',
        'content': encoded,
    }
    if sha:
        payload['sha'] = sha
    r = requests.put(url, json=payload, headers={'Authorization': f'token {GH_TOKEN}'})
    r.raise_for_status()
    print(f'✅ הועלה ל-GitHub: {filename} ({len(content)/1024:.0f} KB)')

# ── הגדרות ──────────────────────────────────────────────
STORE_ID  = 287
BASE_URL  = 'https://prices.shufersal.co.il'
MAX_ITEMS = 300
TODAY     = datetime.now().strftime('%Y-%m-%d')

PRICES_FILENAME  = 'shufersal_prices.json'
HISTORY_FILENAME = 'shufersal_history.json'
DRIVE_FOLDER     = 'מחירוסקופ'

SCOPES = ['https://www.googleapis.com/auth/drive']

# ── Google Drive ─────────────────────────────────────────
def get_drive_service():
    creds_json = os.environ.get('GDRIVE_CREDENTIALS')
    if not creds_json:
        raise ValueError('GDRIVE_CREDENTIALS לא מוגדר')
    creds_info = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds)

def get_or_create_folder(service, name):
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res = service.files().list(q=q, fields='files(id,name)').execute()
    if res['files']:
        return res['files'][0]['id']
    meta = {'name': name, 'mimeType': 'application/vnd.google-apps.folder'}
    f = service.files().create(body=meta, fields='id').execute()
    print(f'📁 תיקיה נוצרה: {name}')
    return f['id']

def read_from_drive(service, folder_id, filename):
    q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    res = service.files().list(q=q, fields='files(id,name)').execute()
    if not res['files']:
        return None
    file_id = res['files'][0]['id']
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, service.files().get_media(fileId=file_id))
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return json.loads(buf.read().decode('utf-8'))

def write_to_drive(service, folder_id, filename, data):
    content = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype='application/json')
    q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    existing = service.files().list(q=q, fields='files(id)').execute()
    if existing['files']:
        service.files().update(fileId=existing['files'][0]['id'], media_body=media).execute()
    else:
        meta = {'name': filename, 'parents': [folder_id]}
        service.files().create(body=meta, media_body=media, fields='id').execute()
    print(f'💾 נשמר ל-Drive: {filename} ({len(content)/1024:.0f} KB)')

# ── שליפת XML ────────────────────────────────────────────
session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0', 'Referer': BASE_URL})

def get_file_links(cat_id):
    url = f'{BASE_URL}/FileObject/UpdateCategory'
    r = session.get(url, params={'catID': cat_id, 'storeId': STORE_ID, 'sort': 1, 'order': 1}, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if any(x in href for x in ['.gz', '.xml', 'Download', 'download']):
            links.append(href if href.startswith('http') else BASE_URL + href)
    return links

def download_content(url):
    print(f'  📥 {url.split("/")[-1].split("?")[0][:60]}')
    r = session.get(url, timeout=120)
    r.raise_for_status()
    content = r.content
    if content[:2] == b'\x1f\x8b':
        content = gzip.decompress(content)
    for enc in ['utf-8', 'windows-1255', 'iso-8859-8']:
        try:
            return content.decode(enc)
        except:
            continue
    return content.decode('utf-8', errors='replace')

def safe_parse_xml(text):
    parts = [p.strip() for p in re.split(r'(?=<\?xml)', text.strip()) if p.strip()]
    roots = []
    for part in parts:
        try:
            roots.append(ET.fromstring(part))
        except ET.ParseError as e:
            print(f'  ⚠️ דילוג: {e}')
    return roots

# ── ניתוח מחירים ─────────────────────────────────────────
def parse_quantity(name):
    n = name.lower()
    for pattern, mult, utype in [
        (r'(\d+[.,]?\d*)\s*(?:ק"ג|קג|kg)',    1000, 'g'),
        (r'(\d+[.,]?\d*)\s*(?:גרם|גר\b|gr\b)', 1,   'g'),
        (r'(\d+[.,]?\d*)\s*(?:ליטר|לטר)',       1000, 'ml'),
        (r'(\d+[.,]?\d*)\s*(?:מ"ל|מל|ml)',      1,   'ml'),
    ]:
        m = re.search(pattern, n)
        if m:
            qty = float(m.group(1).replace(',', '.')) * mult
            if qty > 0:
                return qty, utype
    return None, None

def extract_items(roots):
    products = []
    for root in roots:
        for item in list(root.iter('Item')) or list(root.iter('item')):
            def g(tag):
                for t in [tag, tag.lower(), tag.capitalize()]:
                    el = item.find(t)
                    if el is not None and el.text:
                        return el.text.strip()
                return ''
            name      = g('ItemName') or g('item_name')
            price_str = g('ItemPrice') or g('Price')
            barcode   = g('ItemCode') or g('Barcode')
            unit      = g('UnitOfMeasure') or g('unit_of_measure')
            brand     = g('ManufacturerName') or g('ManufacturerItemDescription')
            updated   = g('PriceUpdateDate')
            if not name or not price_str:
                continue
            try:
                price = round(float(price_str), 2)
                if price <= 0: continue
            except:
                continue
            qty, utype = parse_quantity(name)
            products.append({
                'barcode': barcode, 'name': name, 'price': price,
                'unit': unit, 'unitType': utype, 'qty': qty,
                'pricePer100': round(price / qty * 100, 2) if qty else None,
                'brand': brand, 'updatedAt': updated,
                'promo': None, 'promoPrice': None,
            })
    return products

def extract_promos(roots):
    promos = {}
    for root in roots:
        for promo in root.iter('Promotion'):
            desc = promo.findtext('PromotionDescription') or promo.findtext('RewardDescription') or ''
            for item in promo.iter('Item'):
                code = item.findtext('ItemCode') or item.findtext('Barcode') or ''
                pp   = item.findtext('ItemPrice') or ''
                if code:
                    promos[code] = {'promo': desc, 'promoPrice': float(pp) if pp else None}
    return promos

# ── היסטוריה ─────────────────────────────────────────────
def update_history(history, products):
    new, updated = 0, 0
    for p in products:
        bc = p['barcode']
        if not bc: continue
        if bc not in history:
            history[bc] = {
                'name': p['name'], 'brand': p['brand'],
                'unit': p['unit'], 'unitType': p['unitType'],
                'qty': p['qty'], 'prices': {}
            }
            new += 1
        if TODAY not in history[bc]['prices']:
            history[bc]['prices'][TODAY] = p['price']
            updated += 1
        if p.get('pricePer100'):
            history[bc]['pricePer100'] = p['pricePer100']
    return new, updated

def get_trend(bc, history):
    prices = history.get(bc, {}).get('prices', {})
    dates  = sorted(prices)
    if len(dates) < 2: return None, None, None
    curr, prev = prices[dates[-1]], prices[dates[-2]]
    pct  = round((curr - prev) / prev * 100, 1) if prev else None
    from datetime import date
    days = (date.fromisoformat(dates[-1]) - date.fromisoformat(dates[-2])).days
    return prev, pct, days

def avg_similar(bc, history):
    h     = history.get(bc, {})
    utype = h.get('unitType')
    p100  = h.get('pricePer100')
    if not utype or not p100: return None, 0
    words = set(h.get('name', '').split()[:3])
    vals  = [
        v['pricePer100'] for k, v in history.items()
        if k != bc and v.get('unitType') == utype
        and v.get('pricePer100')
        and words & set(v.get('name', '').split()[:3])
    ]
    if not vals: return None, 0
    return round(sum(vals) / len(vals), 2), len(vals)

# ── קיטלוג ───────────────────────────────────────────────
CATEGORIES = {
    'dairy':     ['חלב','גבינה','יוגורט','שמנת','חמאה','קוטג','לבן','בולגרית','מוצרלה','קשקבל'],
    'meat':      ['עוף','בשר','הודו','כבש','בקר','נקניק','שניצל','קציצ','סטייק','פילה','כנפיים','חזה'],
    'produce':   ['עגבניה','מלפפון','גזר','בצל','תפוח','בננה','תפוז','לימון','פרי','תות','ענב','אבוקדו','חסה','פלפל'],
    'bakery':    ['לחם','פיתה','בגט','חלה','עוגה','עוגיה','מאפה','כיכר','לחמניה','קרואסון','טוסט'],
    'beverages': ['מיץ','קולה','מים','בירה','יין','קפה','תה','שתיה','ספרייט','פנטה','נביעות','סודה'],
    'snacks':    ['חטיף','ביסקוויט','שוקולד','סוכרייה','ממתק','פופקורן','קרקר','אגוז','שקדים','גרנולה'],
    'oil':       ['שמן','רוטב','חומץ','מיונז','קטשופ','חרדל','טחינה','חומוס','פסטו'],
    'grains':    ['אורז','פסטה','קוסקוס','קמח','עדשים','שעועית','בורגול','קינואה','ספגטי','מקרוני'],
    'frozen':    ['קפוא','פיצה','שוורמה','גלידה','מוקפא'],
    'hygiene':   ['שמפו','סבון','קרם','דאודורנט','משחת שיניים','טיפוח','לוסיון'],
    'cleaning':  ['אבקת כביסה','נוזל כביסה','ממיס','אקונומיקה','ניקוי','ספוג','שקית אשפה'],
    'baby':      ['תינוק','פמפרס','חיתול','מוצץ','פורמולה'],
}

def categorize(name, brand):
    text = (name + ' ' + brand).lower()
    for cat, kws in CATEGORIES.items():
        if any(kw in text for kw in kws): return cat
    return 'other'

# ── ראשי ─────────────────────────────────────────────────
def main():
    print(f'🚀 מחירוסקופ — {TODAY}')
    print(f'⚙️  סניף {STORE_ID} — שלי גבעתיים שביט')

    # Drive
    service   = get_drive_service()
    folder_id = get_or_create_folder(service, DRIVE_FOLDER)
    print(f'📁 Drive folder: {folder_id}')

    # טעינת היסטוריה
    history = read_from_drive(service, folder_id, HISTORY_FILENAME) or {}
    all_dates = set(d for h in history.values() for d in h.get('prices', {}))
    print(f'📂 היסטוריה: {len(history):,} מוצרים, {len(all_dates)} ימים')

    # שליפת מחירים
    print('\n🔍 מחפש קבצים...')
    price_links = get_file_links(2)
    promo_links = get_file_links(3)
    print(f'✅ מחירים: {len(price_links)} | מבצעים: {len(promo_links)}')

    full_p = [l for l in price_links if 'full' in l.lower() or 'Full' in l]
    text   = download_content(full_p[0] if full_p else price_links[0])
    roots  = safe_parse_xml(text)
    all_products = extract_items(roots)
    print(f'✅ {len(all_products):,} מוצרים')

    if promo_links:
        full_r = [l for l in promo_links if 'full' in l.lower()]
        text   = download_content(full_r[0] if full_r else promo_links[0])
        roots  = safe_parse_xml(text)
        pd     = extract_promos(roots)
        cnt    = sum(1 for p in all_products if p['barcode'] in pd and p.update(pd[p['barcode']]) is None)
        print(f'🏷️  {cnt} מבצעים')

    # עדכון היסטוריה
    new, updated = update_history(history, all_products)
    print(f'📅 {new} מוצרים חדשים | {updated} מחירים נוספו')
    # ירידות + השוואה
    drops, rises = [], []
    for p in all_products:
        bc = p['barcode']
        if not bc: continue
        prev_price, pct, days = get_trend(bc, history)
        avg, n = avg_similar(bc, history)
        vs_avg = round((p['pricePer100'] - avg) / avg * 100, 1) if avg and p.get('pricePer100') else None
        p.update({'prevPrice': prev_price, 'changePct': pct, 'daysAgo': days,
                  'avgSimilarPer100': avg, 'vsAvgPct': vs_avg, 'nSimilar': n})
        if pct and pct < -1:  drops.append(p)
        elif pct and pct > 1: rises.append(p)

    print(f'📉 {len(drops)} ירידות | 📈 {len(rises)} עליות')

    # קיטלוג
    categorized = defaultdict(list)
    for p in all_products:
        cat = categorize(p['name'], p.get('brand', ''))
        if len(categorized[cat]) < MAX_ITEMS:
            categorized[cat].append(p)

    all_dates = set(d for h in history.values() for d in h.get('prices', {}))
    output = {
        'storeId':        str(STORE_ID),
        'storeName':      'שלי גבעתיים שביט',
        'generatedAt':    datetime.now().isoformat(),
        'today':          TODAY,
        'historyDays':    len(all_dates),
        'historyDates':   sorted(all_dates),
        'totalProducts':  len(all_products),
        'totalWithPromo': sum(1 for p in all_products if p.get('promo')),
        'totalDrops':     len(drops),
        'categories':     dict(categorized),
        'drops':          sorted(drops, key=lambda x: x['changePct'])[:100],
        'rises':          sorted(rises, key=lambda x: -x['changePct'])[:50],
    }

    # היסטוריה נשמרת ב-Drive
    write_to_drive(service, folder_id, HISTORY_FILENAME, history)

    # מחירים מועלים ל-GitHub לאפליקציה
    github_upload('shufersal_prices.json', output)
    print(f'\n🎉 סיום! {len(all_products):,} מוצרים | {len(drops)} ירידות | {len(all_dates)} ימי היסטוריה')

if __name__ == '__main__':
    main()
