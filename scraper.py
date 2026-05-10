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

def github_upload(filename, data):
    """מעלה קובץ JSON ל-GitHub Repository"""
    content_bytes = json.dumps(data, ensure_ascii=False, separators=(',',':')).encode('utf-8')
    encoded = base64.b64encode(content_bytes).decode('utf-8')
    
    headers = {
        'Authorization': f'token {GH_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }
    url = f'{GH_API}/repos/{GH_REPO}/contents/{filename}'
    
    # בדיקה אם הקובץ קיים — לקבלת SHA
    r = requests.get(url, headers=headers)
    payload = {
        'message': f'עדכון מחירים {datetime.now().strftime("%Y-%m-%d %H:%M")}',
        'content': encoded,
    }
    if r.status_code == 200:
        payload['sha'] = r.json()['sha']
        print(f'  📝 מעדכן קובץ קיים...')
    else:
        print(f'  📄 יוצר קובץ חדש...')
    
    r2 = requests.put(url, json=payload, headers=headers)
    if r2.status_code not in [200, 201]:
        print(f'  ❌ שגיאה: {r2.status_code} — {r2.text[:200]}')
        r2.raise_for_status()
    print(f'✅ הועלה ל-GitHub: {filename} ({len(content_bytes)/1024:.0f} KB)')

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
def categorize(name, brand):
    """
    קיטלוג חכם לפי סדר עדיפויות ומילים שלמות.
    הסדר קריטי — הכלל הראשון שמתאים מנצח.
    """
    n = name

    # ── חריגים ברורים ──
    if any(w in n for w in ['קרפרי','חיתול','פמפרס','מוצץ','תינוק','פורמולה']):
        return 'baby'
    if any(w in n for w in ['אקונומיקה','סנו ','ג'אוול','מסיר כתמים','אבקת כביסה',
                             'נוזל כביסה','מרכך ','שקית אשפה','כדוריות אסלה',
                             'סבון אסלה','מטהר אוויר','סיליט','XPO','ג'ל אסלה',
                             'ת.אקונומיקה','מ.ניקוי','ספוג ','נוזל כלים','פיירי ']):
        return 'cleaning'
    if any(w in n for w in ['שמפו','קרם גוף','תחליב גוף','לוסיון','דאו.',
                             'ספיד סטיק','ג'ילט ','ונוס ','קלינקס','טישו ',
                             'מגבונ','ספרי הגנה','SPF','גרנייר','ניוטרוגינ',
                             'ניטרוגינ','וזלין','סקין נט','אלביב מרכך']):
        return 'hygiene'

    # ── אלכוהול ──
    if any(w in n for w in ['וודקה','ויסקי','וויסקי','ליקר ','שמפניה','פרוסקו','בריזר']):
        return 'alcohol'
    if 'בירה ' in n and 'ללא אלכוהול' not in n:
        return 'alcohol'
    if 'יין ' in n and not any(w in n for w in ['מיונז','חומץ','רוטב']):
        return 'alcohol'
    if 'סיידר' in n and 'אלכוהולי' in n:
        return 'alcohol'

    # ── חיות מחמד ──
    if any(w in n for w in ['פריסקיז','פדיגרי','ויסקס']):
        return 'pets'

    # ── בשר ועוף ──
    if any(w in n for w in ['עוף ','חזה עוף','שוקיים','כנפיים','שניצל','פרגית','הודו ']):
        return 'meat'
    if any(w in n for w in ['בשר בקר','בשר טחון','סטייק','אנטריקוט','שריר הזרוע',
                             'נקניק','סלמי','פסטרמה','קבב','המבורגר','נקניקיה','מרגז','בורגר']):
        return 'meat'
    if any(w in n for w in ['רוסטביף','קורנדביף','סרוולד']):
        return 'meat'

    # ── דגים ──
    if any(w in n for w in ['טונה','סרדין','סלמון','הרינג','בקלה']):
        return 'fish'

    # ── מוצרי חלב ──
    if any(w in n for w in ['חלב ','גבינ','יוגורט','שמנת','חמאה','קוטג'','לבן ',
                             'מוצרלה','ריקוטה','קשקבל','פטה ','בולגרית','דנונה',
                             'דניאלה','מולר','יוג.','יופלה','אירן ','אקטיביה']):
        return 'dairy'

    # ── ירקות ──
    veggie_exact = ['עגבניה','מלפפון ','גזר ','חסה ','פלפל ירוק','פלפל אדום',
                    'פלפל צהוב','פלפל כתום','פלפל רמירו','פלפל חריף','פלפל שישקה',
                    'פלפל פאלמרו','פלפל צ'ילי','בצל ירוק','בצל יבש','בצל אדום',
                    'שאלוט','תפוח אדמה','חציל ','קישוא','כרוב ','ברוקולי','כרובית',
                    'תרד ','בטטה ','עגבניה תמר','עגבניה שרי','מארז פלפל',
                    'גזר ארוז','גזר תפזורת','כרוב אדום','כרוב לבן','תפרחות','קישואים',
                    'מארז עגבנ','חטיפוני גזר','מלפפון אורג']
    if any(w in n for w in veggie_exact):
        if not any(ex in n for ex in ['סלט','ממרח','במיונז','בטחינה','פיקנטי','כיסונים','מטוגן']):
            return 'produce'

    # ── פירות ──
    fruit_exact = ['תפוח עץ','אגס ','אפרסק טרי','ענבים שחורים','ענב אדום',
                   'ענב ירוק','ענב ארלי','בננה מובחרת','בננה אורגנית',
                   'לימון ','לימון אורגני','תפוז מובחר','תפוז ברשת',
                   'מארז אבוקדו','אבוקדו ','תות שדה','אשכולית ']
    if any(w in n for w in fruit_exact):
        if not any(ex in n for ex in ['מיץ','סירופ','שוופס','ספרייט','ג'לי',
                                       'מסטיק','סוכרייה','לוקיטוס','ספרי','גלידל',
                                       'ופלים','חליטה','רוטב','קונפיטורה','מעדן']):
            return 'produce'

    # ── לחם ומאפים ──
    if any(w in n for w in ['לחם ','פיתה ','פיתות','בגט','חלה ','לחמניה','לחמית',
                             'קרואסון','לחם מחמצת','ג'בטה','כיכר ']):
        return 'bakery'

    # ── עוגות ועוגיות ──
    if any(w in n for w in ['עוגיה','ביסקוויט','ופלים','קרקר ','פתית ','ערגליות']):
        return 'cookies'

    # ── שוקולד ──
    if any(w in n for w in ['שוקולד','פסק זמן','סניקרס','באונטי','לינדט',
                             'קינדר','מילקה','טובלרון','מארס ','טוויקס']):
        return 'chocolate'

    # ── סוכריות ──
    if any(w in n for w in ['סוכרייה','מסטיק ','טיק טק','סקיטלס','ג'לי ','לוקיטוס','חמצוץ']):
        return 'candy'

    # ── חטיפים מלוחים ──
    if any(w in n for w in ['דוריטוס','פרינגלס','תפוציפס','תפוצ'יפס',
                             'ציפס','חטיף ','בייגלה','פריכיות','פריכונים','ציטוס','כיפלי']):
        return 'snacks'

    # ── קפה ──
    if any(w in n for w in ['קפה ','עלית טורקי','קפה נמס','קפסולות','נספרסו','אספרסו']):
        return 'coffee'

    # ── תה ──
    if any(w in n for w in ['תה ','חליטה','חליטת']):
        return 'tea'

    # ── משקאות ──
    if any(w in n for w in ['מיץ ','פריגת','ספרינג ','ספרייט','קולה ','פנטה',
                             'נביעות','מי עדן','מים מינרל','מים מוגז','מים טעם',
                             'מים מצוננ','מים ','שוופס','פיוז-טי','ג'אמפ ',
                             'תפוזינה','אושן ספריי','סחוט ','משקה ','סודה ','ציזיקי']):
        return 'beverages'

    # ── שמנים ורטבים ──
    if any(w in n for w in ['שמן זית','שמן קנולה','שמן חמנ','שמן ','רוטב ',
                             'קטשופ','מיונז','חרדל','חומץ ','פסטו','טחינה','ממרח ']):
        return 'oil'

    # ── דגנים ──
    if any(w in n for w in ['אורז ','פסטה ','ספגטי','מקרוני','קוסקוס','בורגול',
                             'קינואה','עדשים','שעועית ','קמח ','פתיתים','אטריות','כוסמת']):
        return 'grains'

    # ── ארוחת בוקר ──
    if any(w in n for w in ['דגני בוקר','קורנפלקס','קורני ','גרנולה ',
                             'ממרח השחר','נוטלה','דבש ','ריבה ','קונפיטורה','חמאת בוטנ']):
        return 'breakfast'

    # ── קפואים ──
    if any(w in n for w in ['קפוא','מוקפא','גלידה','גלידל','שלגון','שרבט']):
        return 'frozen'

    # ── שימורים ──
    if any(w in n for w in ['גרגירי חומוס','תירס קל','עגבניות מרוסקות',
                             'רוטב עגבניות','פריכוז','פריניר','מלפפון חומץ','זית ']):
        return 'pantry'

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
