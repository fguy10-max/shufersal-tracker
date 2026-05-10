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

STORES = [
    {'id':'sheli_shabit','name':'שלי גבעתיים שביט','short':'שלי שביט','source':'shufersal','store_id':287},
    {'id':'express_histadrut','name':'שופרסל אקספרס ההסתדרות','short':'אקספרס','source':'shufersal','store_id':599},
    {'id':'citymarket_givataim','name':'סיטי מרקט גבעתיים','short':'סיטי מרקט','source':'citymarket','store_branch':'079'},
]

TODAY   = datetime.now().strftime('%Y-%m-%d')
GH_TOKEN = os.environ.get('GH_TOKEN')
GH_REPO  = 'fguy10-max/shufersal-tracker'
GH_API   = 'https://api.github.com'
SHUFERSAL_BASE  = 'https://prices.shufersal.co.il'
CITYMARKET_BASE = 'https://www.citymarket-shops.co.il'
DRIVE_FOLDER = 'מחירוסקופ'
HISTORY_FILE = 'shufersal_history.json'
OUTPUT_FILE  = 'shufersal_prices.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

# Drive
def get_drive_service():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(os.environ['GDRIVE_CREDENTIALS']), scopes=SCOPES)
    return build('drive', 'v3', credentials=creds)

def get_or_create_folder(service, name):
    q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    res = service.files().list(q=q, fields='files(id)').execute()
    if res['files']: return res['files'][0]['id']
    return service.files().create(body={'name':name,'mimeType':'application/vnd.google-apps.folder'},fields='id').execute()['id']

def read_from_drive(service, folder_id, filename):
    q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    res = service.files().list(q=q, fields='files(id)').execute()
    if not res['files']: return None
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, service.files().get_media(fileId=res['files'][0]['id']))
    done = False
    while not done: _, done = dl.next_chunk()
    buf.seek(0)
    return json.loads(buf.read().decode('utf-8'))

def write_to_drive(service, folder_id, filename, data):
    content = json.dumps(data, ensure_ascii=False, separators=(',',':')).encode('utf-8')
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype='application/json')
    q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    ex = service.files().list(q=q, fields='files(id)').execute()
    if ex['files']: service.files().update(fileId=ex['files'][0]['id'], media_body=media).execute()
    else: service.files().create(body={'name':filename,'parents':[folder_id]}, media_body=media, fields='id').execute()
    print(f'  Drive: {filename} ({len(content)/1024:.0f} KB)')

# GitHub
def github_upload(filename, data):
    content_bytes = json.dumps(data, ensure_ascii=False, separators=(',',':')).encode('utf-8')
    encoded = base64.b64encode(content_bytes).decode('utf-8')
    headers = {'Authorization':f'token {GH_TOKEN}','Accept':'application/vnd.github.v3+json'}
    url = f'{GH_API}/repos/{GH_REPO}/contents/{filename}'
    r = requests.get(url, headers=headers)
    payload = {'message':f'עדכון {TODAY}','content':encoded}
    if r.status_code == 200: payload['sha'] = r.json()['sha']
    r2 = requests.put(url, json=payload, headers=headers)
    if r2.status_code not in [200,201]: r2.raise_for_status()
    print(f'  GitHub: {filename} ({len(content_bytes)/1024:.0f} KB)')

# XML
def download_content(url):
    print(f'    {url.split("/")[-1].split("?")[0][:60]}')
    r = session.get(url, timeout=120); r.raise_for_status()
    content = r.content
    if content[:2] == b'\x1f\x8b': content = gzip.decompress(content)
    for enc in ['utf-8','windows-1255','iso-8859-8']:
        try: return content.decode(enc)
        except: pass
    return content.decode('utf-8', errors='replace')

def safe_parse_xml(text):
    parts = [p.strip() for p in re.split(r'(?=<\?xml)', text.strip()) if p.strip()]
    roots = []
    for part in parts:
        try: roots.append(ET.fromstring(part))
        except ET.ParseError: pass
    return roots

def parse_quantity(name):
    n = name.lower()
    for pattern, mult, utype in [
        (r'(\d+[.,]?\d*)\s*(?:ק"ג|קג|kg)', 1000,'g'),
        (r'(\d+[.,]?\d*)\s*(?:גרם|גר\b|gr\b)', 1,'g'),
        (r'(\d+[.,]?\d*)\s*(?:ליטר|לטר)', 1000,'ml'),
        (r'(\d+[.,]?\d*)\s*(?:מ"ל|מל|ml)', 1,'ml'),
    ]:
        m = re.search(pattern, n)
        if m:
            qty = float(m.group(1).replace(',','.')) * mult
            if qty > 0: return qty, utype
    return None, None

def extract_items(roots):
    products = []
    for root in roots:
        items = list(root.iter('Item')) or list(root.iter('item'))
        for item in items:
            def g(tag):
                for t in [tag, tag.lower(), tag.capitalize()]:
                    el = item.find(t)
                    if el is not None and el.text: return el.text.strip()
                return ''
            name=g('ItemName') or g('item_name'); price_str=g('ItemPrice') or g('Price')
            barcode=g('ItemCode') or g('Barcode'); unit=g('UnitOfMeasure') or g('unit_of_measure')
            brand=g('ManufacturerName') or g('ManufacturerItemDescription'); updated=g('PriceUpdateDate')
            if not name or not price_str: continue
            try:
                price = round(float(price_str), 2)
                if price <= 0: continue
            except: continue
            qty, utype = parse_quantity(name)
            products.append({'barcode':barcode,'name':name,'price':price,'unit':unit,
                'unitType':utype,'qty':qty,'pricePer100':round(price/qty*100,2) if qty else None,
                'brand':brand,'updatedAt':updated,'promo':None,'promoPrice':None})
    return products

def extract_promos(roots):
    promos = {}
    for root in roots:
        for promo in root.iter('Promotion'):
            desc = promo.findtext('PromotionDescription') or promo.findtext('RewardDescription') or ''
            for item in promo.iter('Item'):
                code = item.findtext('ItemCode') or item.findtext('Barcode') or ''
                pp = item.findtext('ItemPrice') or ''
                if code: promos[code] = {'promo':desc,'promoPrice':float(pp) if pp else None}
    return promos

# Scrapers
def scrape_shufersal(store_id):
    def get_links(cat_id):
        r = session.get(f'{SHUFERSAL_BASE}/FileObject/UpdateCategory',
            params={'catID':cat_id,'storeId':store_id,'sort':1,'order':1}, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if any(x in href for x in ['.gz','.xml','Download','download']):
                links.append(href if href.startswith('http') else SHUFERSAL_BASE + href)
        return links
    price_links = get_links(2); promo_links = get_links(3)
    print(f'  מחירים: {len(price_links)} | מבצעים: {len(promo_links)}')
    if not price_links: return [], {}
    full = [l for l in price_links if 'full' in l.lower() or 'Full' in l]
    products = extract_items(safe_parse_xml(download_content(full[0] if full else price_links[0])))
    if promo_links:
        full_p = [l for l in promo_links if 'full' in l.lower()]
        pd = extract_promos(safe_parse_xml(download_content(full_p[0] if full_p else promo_links[0])))
        cnt = sum(1 for p in products if p['barcode'] in pd and p.update(pd[p['barcode']]) is None)
        print(f'  {cnt} מבצעים')
    return products, {}

def scrape_citymarket(branch):
    """
    מושך מחירים מסיטי מרקט דרך Bina Projects API.
    branch = מספר סניף (למשל '079')
    """
    BINA_BASE = 'https://citymarketkiryatgat.binaprojects.com'
    API_URL   = f'{BINA_BASE}/MainIO_Hok.aspx'

    import zipfile, io as _io, json as _json

    def bina_download(filename):
        """מוריד קובץ מ-Bina — תומך ב-gzip וב-ZIP"""
        url = f'{BINA_BASE}/MainIO_Hok.aspx?WFileName={filename}'
        print(f'    {filename}')
        r = session.get(url, timeout=120)
        r.raise_for_status()
        raw = r.content

        def decode_bytes(data):
            for enc in ['utf-8', 'windows-1255', 'iso-8859-8']:
                try:
                    return data.decode(enc)
                except:
                    pass
            return data.decode('utf-8', errors='replace')

        # gzip
        if raw[:2] == b'\x1f\x8b':
            import gzip as _gz
            return decode_bytes(_gz.decompress(raw))

        # ZIP
        try:
            with zipfile.ZipFile(_io.BytesIO(raw)) as z:
                for name in z.namelist():
                    if name.endswith('.xml'):
                        return decode_bytes(z.read(name))
        except Exception:
            pass

        # XML ישיר
        return decode_bytes(raw)


    # קבלת רשימת קבצים לסניף
    print(f'  מחפש קבצים לסניף {branch}...')
    price_file = promo_file = None

    for file_type, param in [('מחירים', 2), ('מבצעים', 3)]:
        r = session.get(API_URL, params={
            'WFileType': param, 'WStore': 0, 'WBranch': branch
        }, timeout=30)
        r.raise_for_status()
        files = _json.loads(r.text)

        # מסנן לסניף הספציפי ובוחר הכי עדכני
        branch_files = [
            f for f in files
            if f'-{branch}-' in f['FileNm']
        ]
        if branch_files:
            # הכי עדכני = ראשון ברשימה
            latest = branch_files[0]['FileNm']
            if file_type == 'מחירים':
                price_file = latest
            else:
                promo_file = latest
            print(f'  {file_type}: {latest}')

    if not price_file:
        print(f'  ⚠️ לא נמצא קובץ מחירים')
        return [], {}

    # הורדה וניתוח
    xml_text = bina_download(price_file)
    roots    = safe_parse_xml(xml_text)
    products = extract_items(roots)
    print(f'  {len(products):,} מוצרים')

    if promo_file:
        xml_promo = bina_download(promo_file)
        roots_p   = safe_parse_xml(xml_promo)
        pd        = extract_promos(roots_p)
        cnt = sum(1 for p in products if p['barcode'] in pd and p.update(pd[p['barcode']]) is None)
        print(f'  {cnt} מבצעים')

    return products, {}


# Categorize
def categorize(name, brand):
    n = name
    if any(w in n for w in ['קרפרי','חיתול','פמפרס','מוצץ','תינוק','פורמולה']): return 'baby'
    if any(w in n for w in ['אקונומיקה','סנו ','מסיר כתמים','אבקת כביסה','נוזל כביסה','מרכך ',
                             'שקית אשפה','כדוריות אסלה','סבון אסלה','מטהר אוויר','סיליט','XPO',
                             'ת.אקונומיקה','מ.ניקוי','ספוג ','נוזל כלים','פיירי ']): return 'cleaning'
    if any(w in n for w in ['שמפו','קרם גוף','תחליב גוף','לוסיון','דאו.','ספיד סטיק',
                             'קלינקס','טישו ','מגבונ','ספרי הגנה','SPF','גרנייר',
                             'ניוטרוגינ','ניטרוגינ','וזלין','סקין נט']): return 'hygiene'
    if any(w in n for w in ['וודקה','ויסקי','וויסקי','ליקר ','שמפניה','פרוסקו','בריזר']): return 'alcohol'
    if 'בירה ' in n and 'ללא אלכוהול' not in n: return 'alcohol'
    if 'יין ' in n and not any(w in n for w in ['מיונז','חומץ','רוטב']): return 'alcohol'
    if any(w in n for w in ['פריסקיז','פדיגרי','ויסקס']): return 'pets'
    if any(w in n for w in ['עוף ','חזה עוף','שוקיים','כנפיים','שניצל','פרגית','הודו ']): return 'meat'
    if any(w in n for w in ['בשר בקר','בשר טחון','סטייק','אנטריקוט','נקניק','סלמי',
                             'פסטרמה','קבב','המבורגר','נקניקיה','מרגז','בורגר']): return 'meat'
    if any(w in n for w in ['טונה','סרדין','סלמון','הרינג','בקלה']): return 'fish'
    if any(w in n for w in ['חלב ','גבינ','יוגורט','שמנת','חמאה','לבן ','מוצרלה','ריקוטה',
                             'קשקבל','בולגרית','דנונה','דניאלה','מולר','יוג.','יופלה','אירן ','אקטיביה']): return 'dairy'
    veggie=['עגבניה','מלפפון ','גזר ','חסה ','פלפל ירוק','פלפל אדום','פלפל צהוב','פלפל כתום',
            'בצל ירוק','בצל יבש','בצל אדום','שאלוט','תפוח אדמה','חציל ','קישוא',
            'כרוב ','ברוקולי','כרובית','תרד ','בטטה ','עגבניה תמר','עגבניה שרי','גזר ארוז']
    if any(w in n for w in veggie):
        if not any(ex in n for ex in ['סלט','ממרח','במיונז','כיסונים','מטוגן']): return 'produce'
    fruit=['תפוח עץ','אגס ','ענבים שחורים','ענב אדום','ענב ירוק','בננה מובחרת','בננה אורגנית',
           'לימון ','לימון אורגני','תפוז מובחר','מארז אבוקדו','אבוקדו ','תות שדה','אשכולית ']
    if any(w in n for w in fruit):
        if not any(ex in n for ex in ['מיץ','סירופ','שוופס','ספרייט','מסטיק','חליטה','רוטב']): return 'produce'
    if any(w in n for w in ['לחם ','פיתה ','פיתות','בגט','חלה ','לחמניה','לחמית','קרואסון','כיכר ']): return 'bakery'
    if any(w in n for w in ['עוגיה','ביסקוויט','ופלים','קרקר ','פתית ','ערגליות']): return 'cookies'
    if any(w in n for w in ['שוקולד','פסק זמן','סניקרס','באונטי','לינדט','קינדר','מילקה','טובלרון']): return 'chocolate'
    if any(w in n for w in ['סוכרייה','מסטיק ','טיק טק','סקיטלס','לוקיטוס','חמצוץ']): return 'candy'
    if any(w in n for w in ['דוריטוס','פרינגלס','תפוציפס','ציפס','חטיף ','בייגלה','פריכיות','ציטוס','כיפלי']): return 'snacks'
    if any(w in n for w in ['קפה ','עלית טורקי','קפה נמס','קפסולות','נספרסו','אספרסו']): return 'coffee'
    if any(w in n for w in ['תה ','חליטה','חליטת']): return 'tea'
    if any(w in n for w in ['מיץ ','פריגת','ספרינג ','ספרייט','קולה ','פנטה','נביעות','מי עדן',
                             'מים מינרל','מים מוגז','מים טעם','מים ','שוופס','תפוזינה','סחוט ','משקה ','סודה ']): return 'beverages'
    if any(w in n for w in ['שמן זית','שמן קנולה','שמן חמנ','שמן ','רוטב ','קטשופ','מיונז',
                             'חרדל','חומץ ','פסטו','טחינה','ממרח ']): return 'oil'
    if any(w in n for w in ['אורז ','פסטה ','ספגטי','מקרוני','קוסקוס','בורגול','קינואה',
                             'עדשים','שעועית ','קמח ','פתיתים','אטריות']): return 'grains'
    if any(w in n for w in ['דגני בוקר','קורנפלקס','קורני ','גרנולה ','ממרח השחר','נוטלה',
                             'דבש ','ריבה ','קונפיטורה','חמאת בוטנ']): return 'breakfast'
    if any(w in n for w in ['קפוא','מוקפא','גלידה','גלידל','שלגון','שרבט']): return 'frozen'
    if any(w in n for w in ['גרגירי חומוס','תירס קל','עגבניות מרוסקות','מלפפון חומץ','זית ']): return 'pantry'
    return 'other'

# History + Trend
def update_history(history, store_id, products):
    if store_id not in history: history[store_id] = {}
    sh = history[store_id]; new = updated = 0
    for p in products:
        bc = p['barcode']
        if not bc: continue
        if bc not in sh:
            sh[bc] = {'name':p['name'],'brand':p['brand'],'unit':p['unit'],
                      'unitType':p['unitType'],'qty':p['qty'],'prices':{}}
            new += 1
        if TODAY not in sh[bc]['prices']:
            sh[bc]['prices'][TODAY] = p['price']; updated += 1
        if p.get('pricePer100'): sh[bc]['pricePer100'] = p['pricePer100']
    return new, updated

def get_trend(bc, sh):
    prices = sh.get(bc,{}).get('prices',{}); dates = sorted(prices)
    if len(dates) < 2: return None, None, None
    curr, prev = prices[dates[-1]], prices[dates[-2]]
    pct = round((curr-prev)/prev*100,1) if prev else None
    from datetime import date
    days = (date.fromisoformat(dates[-1])-date.fromisoformat(dates[-2])).days
    return prev, pct, days

def avg_similar(bc, sh):
    h = sh.get(bc,{}); utype=h.get('unitType'); p100=h.get('pricePer100')
    if not utype or not p100: return None, 0
    words = set(h.get('name','').split()[:3])
    vals = [v['pricePer100'] for k,v in sh.items()
            if k!=bc and v.get('unitType')==utype and v.get('pricePer100')
            and words & set(v.get('name','').split()[:3])]
    if not vals: return None, 0
    return round(sum(vals)/len(vals),2), len(vals)

# Main
def main():
    print(f'מחירוסקופ רב-סניפי — {TODAY}')
    service = get_drive_service()
    folder_id = get_or_create_folder(service, DRIVE_FOLDER)
    raw_history = read_from_drive(service, folder_id, HISTORY_FILE) or {}
    # בדיקה שהמבנה תואם לגרסה החדשה (חלוקה לסניפים)
    # אם המפתח הראשון הוא ברקוד (מבנה ישן) — מאפסים
    first_key = next(iter(raw_history), None)
    if first_key and first_key not in [s['id'] for s in STORES]:
        print('  ⚠️ היסטוריה ישנה — מאפס ומתחיל מחדש')
        history = {}
    else:
        history = raw_history
    all_dates = set(d for sh in history.values() for h in sh.values() for d in h.get('prices',{}))
    print(f'היסטוריה: {len(all_dates)} ימים')

    stores_data = {}
    for store in STORES:
        print(f'\n{store["name"]}')
        try:
            if store['source'] == 'shufersal':
                products, _ = scrape_shufersal(store['store_id'])
            else:
                products, _ = scrape_citymarket(store['store_branch'])
            print(f'  {len(products):,} מוצרים')
            new, upd = update_history(history, store['id'], products)
            print(f'  {new} חדשים | {upd} עודכנו')
            sh = history.get(store['id'], {})
            for p in products:
                bc = p['barcode']
                prev_price, pct, days = get_trend(bc, sh)
                avg, n = avg_similar(bc, sh)
                vs_avg = round((p['pricePer100']-avg)/avg*100,1) if avg and p.get('pricePer100') else None
                p.update({'prevPrice':prev_price,'changePct':pct,'daysAgo':days,
                          'avgSimilarPer100':avg,'vsAvgPct':vs_avg,'nSimilar':n})
            categorized = defaultdict(list)
            for p in products:
                p['_cat'] = categorize(p['name'], p.get('brand',''))
                categorized[p['_cat']].append(p)
            stores_data[store['id']] = {
                'meta':store,'products':products,'categories':dict(categorized)
            }
        except Exception as e:
            print(f'  שגיאה: {e}')
            stores_data[store['id']] = {'meta':store,'products':[],'categories':{},'error':str(e)}

    # השוואה
    barcode_map = {}
    for store_id, sdata in stores_data.items():
        for p in sdata['products']:
            bc = p['barcode']
            if not bc: continue
            if bc not in barcode_map:
                barcode_map[bc] = {'name':p['name'],'brand':p['brand'],'unit':p['unit'],'prices':{}}
            barcode_map[bc]['prices'][store_id] = p['price']
    multi_store = {bc:d for bc,d in barcode_map.items() if len(d['prices'])>1}
    print(f'\n{len(multi_store):,} מוצרים משותפים')

    write_to_drive(service, folder_id, HISTORY_FILE, history)

    all_dates = set(d for sh in history.values() for h in sh.values() for d in h.get('prices',{}))
    output = {
        'generatedAt': datetime.now().isoformat(),
        'today': TODAY,
        'historyDays': len(all_dates),
        'historyDates': sorted(all_dates),
        'stores': [{
            'id': s['id'], 'name': s['name'], 'short': s['short'],
            'totalProducts': len(stores_data[s['id']]['products']),
            'totalWithPromo': sum(1 for p in stores_data[s['id']]['products'] if p.get('promo')),
            'totalDrops': sum(1 for p in stores_data[s['id']]['products'] if p.get('changePct') and p['changePct']<-1),
            'categories': stores_data[s['id']].get('categories',{}),
            'error': stores_data[s['id']].get('error'),
        } for s in STORES],
        'comparison': {'totalShared': len(multi_store), 'barcodes': multi_store}
    }
    github_upload(OUTPUT_FILE, output)
    print('\nסיום!')

if __name__ == '__main__':
    main()
