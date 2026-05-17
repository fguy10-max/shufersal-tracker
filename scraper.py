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
PROMO_CACHE  = 'promo_cache.json'  # cached PromoFull data
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
            desc      = promo.findtext('PromotionDescription') or promo.findtext('RewardDescription') or ''
            end       = promo.findtext('PromotionEndDateTime') or promo.findtext('PromotionEndDate') or ''
            is_coupon = promo.findtext('AdditionalIsCoupon') or '0'
            # ClubID varies by chain: direct or nested under <Clubs><ClubId>
            club_id   = (promo.findtext('ClubID') or
                         promo.findtext('.//ClubId') or
                         promo.findtext('ClubId') or '')
            club_num  = club_id.strip().split(' ')[0]  # e.g. '0', '3'

            # Skip expired
            if end and end[:10] < TODAY:
                continue
            # Skip credit card / employee clubs (ClubID != 0)
            if club_num and club_num != '0':
                continue
            # Skip irrelevant promos
            if any(w in desc for w in ['פיצוי', 'פיצויים', 'סיבוס', 'SBOX',
                                        'תו זהב', 'ח"ע', 'ח.ע', 'קופון ח']):
                continue

            # Label coupons clearly
            label = f'🎫 קופון: {desc}' if is_coupon == '1' else desc

            # Format 1: Shufersal — <PromotionItem>
            for pitem in promo.iter('PromotionItem'):
                code = pitem.findtext('ItemCode') or ''
                pp   = pitem.findtext('DiscountedPrice') or ''
                if code and code != '0000000000000':
                    promos[code] = {'promo': label, 'promoPrice': float(pp) if pp else None}
            # Format 2: <Item>
            for item in promo.iter('Item'):
                code = item.findtext('ItemCode') or item.findtext('Barcode') or ''
                pp   = item.findtext('ItemPrice') or item.findtext('DiscountedPrice') or ''
                if code and code != '0000000000000':
                    promos[code] = {'promo': label, 'promoPrice': float(pp) if pp else None}
            # Format 3: ItemCode directly in Promotion
            direct = promo.findtext('ItemCode')
            if direct and direct != '0000000000000':
                pp = promo.findtext('DiscountedPrice') or ''
                promos[direct] = {'promo': label, 'promoPrice': float(pp) if pp else None}
    return promos

def scrape_shufersal(store_id, service, folder_id):
    def get_links(cat_id):
        import re
        r = session.get(f'{SHUFERSAL_BASE}/FileObject/UpdateCategory',
            params={'catID':cat_id,'storeId':store_id,'sort':1,'order':1}, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        links = []
        for row in soup.find_all('tr'):
            a = row.find('a', href=True)
            if not a: continue
            href = a['href']
            if not any(x in href for x in ['.gz','.xml','Download','download']): continue
            url = href if href.startswith('http') else SHUFERSAL_BASE + href
            row_text = row.get_text(separator=' ')
            match = re.search(r'((?:Price|Promo)[A-Za-z0-9-_]+[.]gz)', row_text, re.IGNORECASE)
            name = match.group(1) if match else row_text.strip()
            links.append((name, url))
        return links
    price_links = get_links(2); promo_links = get_links(3)
    print(f'  מחירים: {len(price_links)} | מבצעים: {len(promo_links)}')
    if not price_links: return [], {}

    # Sort by filename: full files first, then partial
    def is_full(name): return 'full' in name.lower()

    price_full    = [(n,u) for n,u in price_links if is_full(n)]
    price_partial = [(n,u) for n,u in price_links if not is_full(n)]

    products_dict = {}
    for name, url in price_full + price_partial:
        print(f'    {name}')
        batch = extract_items(safe_parse_xml(download_content(url)))
        for p in batch:
            if p['barcode']:
                products_dict[p['barcode']] = p

    products = list(products_dict.values())
    print(f'  {len(products):,} מוצרים ({len(price_full)} full + {len(price_partial)} partial)')

    pd = {}

    # Step 1: Load PromoFull cache from GitHub
    cache_url = f'https://raw.githubusercontent.com/{GH_REPO}/main/promo_cache_{store_id}.json'
    try:
        r_cache = requests.get(cache_url + '?t=' + TODAY, timeout=30)
        if r_cache.status_code == 200:
            pd = r_cache.json()
            print(f'  📂 PromoFull cache: {len(pd):,} items')
        else:
            print(f'  ⚠️ No PromoFull cache found')
    except Exception as e:
        print(f'  ⚠️ Cache load failed: {e}')

    # Step 2: Try to get PromoFull directly from site (when available)
    promo_full_links = [(n,u) for n,u in promo_links if is_full(n)]
    if promo_full_links:
        for name, url in promo_full_links:
            print(f'    {name} (full - updating cache)')
            fresh = extract_promos(safe_parse_xml(download_content(url)))
            pd.update(fresh)
            # Update cache in GitHub
            try:
                github_upload(f'promo_cache_{store_id}.json', pd)
                print(f'  💾 Cache updated: {len(pd):,} items')
            except Exception as e:
                print(f'  ⚠️ Cache update failed: {e}')

    # Step 3: Merge partial promo updates on top
    promo_partial_links = [(n,u) for n,u in promo_links if not is_full(n)]
    for name, url in promo_partial_links:
        print(f'    {name} (partial)')
        pd.update(extract_promos(safe_parse_xml(download_content(url))))

    print(f'  promo dict size: {len(pd):,}')
    cnt = sum(1 for p in products if p['barcode'] in pd and p.update(pd[p['barcode']]) is None)
    print(f'  {cnt} מבצעים')
    return products, {}

def scrape_citymarket(branch):
    """
    סיטי מרקט מפרסמים עדכונים חלקיים בלבד.
    צוברים את כל הקבצים (מהישן לחדש) לקבלת תמונה מלאה.
    """
    BINA_BASE = 'https://citymarketkiryatgat.binaprojects.com'
    API_URL   = f'{BINA_BASE}/MainIO_Hok.aspx'

    import zipfile, io as _io, json as _json, gzip as _gz

    def bina_download_raw(filename):
        url = f'{BINA_BASE}/download/{filename}'
        print(f'    {filename}')
        r = session.get(url, timeout=120)
        r.raise_for_status()
        raw = r.content
        # ZIP
        if raw[:2] == b'PK':
            with zipfile.ZipFile(_io.BytesIO(raw)) as z:
                data = z.read(z.namelist()[0])
        # gzip
        elif raw[:2] == b'\x1f\x8b':
            data = _gz.decompress(raw)
        else:
            data = raw
        for enc in ['utf-8', 'windows-1255', 'iso-8859-8']:
            try:
                return data.decode(enc)
            except:
                pass
        return data.decode('utf-8', errors='replace')

    def parse_bina_items(xml_text):
        roots = safe_parse_xml(xml_text)
        products = {}
        for root in roots:
            for item in root.iter('Item'):
                def g(tag):
                    for t in [tag, tag.lower(), tag.capitalize()]:
                        el = item.find(t)
                        if el is not None and el.text:
                            return el.text.strip()
                    return ''
                bc        = g('ItemCode') or g('Barcode')
                name      = g('ItemNm') or g('ItemName') or g('item_name')
                price_str = g('ItemPrice') or g('Price')
                unit      = g('UnitOfMeasure') or g('UnitQty')
                brand     = g('ManufacturerName') or g('ManufacturerItemDescription')
                updated   = g('PriceUpdateDate')
                if not bc or not price_str or not name:
                    continue
                try:
                    price = round(float(price_str), 2)
                    if price <= 0:
                        continue
                except:
                    continue
                qty, utype = parse_quantity(name)
                products[bc] = {
                    'barcode': bc, 'name': name, 'price': price,
                    'unit': unit, 'unitType': utype, 'qty': qty,
                    'pricePer100': round(price/qty*100, 2) if qty else None,
                    'brand': brand, 'updatedAt': updated,
                    'promo': None, 'promoPrice': None,
                }
        return products

    # קבלת רשימת קבצים
    print(f'  מחפש קבצים לסניף {branch}...')
    price_files = promo_files = []

    for file_type, param in [('מחירים', 2), ('מבצעים', 5)]:
        r = session.get(API_URL, params={
            'WFileType': param, 'WStore': 0, 'WBranch': branch
        }, timeout=30)
        r.raise_for_status()
        files = _json.loads(r.text)
        branch_files = [f for f in files if f'-{branch}-' in f['FileNm']]
        if file_type == 'מחירים':
            price_files = branch_files
        else:
            promo_files = branch_files
        print(f'  {file_type}: {len(branch_files)} קבצים')

    if not price_files:
        print(f'  ⚠️ לא נמצאו קבצים')
        return [], {}

    # צבירת כל קבצי המחירים מהישן לחדש
    all_products = {}
    for f in reversed(price_files):
        try:
            xml_text = bina_download_raw(f['FileNm'])
            batch = parse_bina_items(xml_text)
            all_products.update(batch)
        except Exception as e:
            print(f'    ⚠️ {f["FileNm"]}: {e}')

    products = list(all_products.values())
    print(f'  ✅ {len(products):,} מוצרים ייחודיים')

    # מבצעים — PromoFull בלבד (הכי עדכני), חלקיים מכילים מבצעים ישנים
    if promo_files:
        try:
            pd = {}
            # Prefer PromoFull file; fall back to most recent partial
            full_files = [f for f in promo_files if 'promofull' in f['FileNm'].lower()]
            files_to_use = full_files if full_files else [promo_files[0]]
            for f in files_to_use:
                try:
                    xml_promo = bina_download_raw(f['FileNm'])
                    roots_p   = safe_parse_xml(xml_promo)
                    pd.update(extract_promos(roots_p))
                    print(f'    {f["FileNm"]} ({len(pd)} promos)')
                except Exception as e:
                    print(f'    ⚠️ {f["FileNm"]}: {e}')
            cnt = sum(1 for p in products if p['barcode'] in pd and p.update(pd[p['barcode']]) is None)
            print(f'  pd size: {len(pd)} | matched: {cnt}')
            print(f'  {cnt} מבצעים')
        except Exception as e:
            print(f'  ⚠️ מבצעים: {e}')

    return products, {}


def categorize(name, brand=""):
    n = name.strip()
    # תינוקות
    if any(w in n for w in ['חיתול','פמפרס','קרפרי','האגיס','מוצץ','תינוק','פורמולה',
                             'מזון לתינוק','דייסת','סימילאק','פרינוק']): return 'baby'
    # ניקיון הבית
    if any(w in n for w in ['אקונומיקה','סנו ','מסיר כתמים','אבקת כביסה','נוזל כביסה',
                             'מרכך בד','מרכך כביסה','שקית אשפה','כדוריות אסלה',
                             'מטהר אוויר','סיליט','XPO','נוזל כלים','פיירי ',
                             'קפסולות פיניש','קפסולות קלגון','נ.כלים',
                             'ג׳ל לניקוי','נוזל לניקוי','ניקוי חלונות',
                             'א.כביסה','סנומט','קוטל חרק','סנובון אסלה',
                             'פינוק מרכך','פינוק 3','מרכך פינוק','תחליב פינוק',
                             'מ.כביסה','ספוג הקסם','ספוג עיסוי','ספוגים','מגבת רב']): return 'cleaning'
    # טיפוח וגהות
    if any(w in n for w in ['שמפו','קרם גוף','תחליב גוף','תחל.','לוסיון',
                             'דאו.','דאו ','ספיד סטיק','קלינקס','טישו ','מגבונ','SPF',
                             'גרנייר','גרנייה','ניוטרוגינ','וזלין','סבון גוף',
                             'קרם ידיים','קרם יד.','דאודורנט','גילוח','סכיני גילוח',
                             'מברשת שינ','מברשות שינ','קולגייט','משחת שיניים',
                             'אולוויז','סקין נט','רויטליפט','ניוואה',
                             'מרכך שיער','קרם לשיער','קרם לחות','אל סבון',
                             'גונסון','להסרת שיער','SPECIALIST','קרם יום','קרם לילה',
                             'סרום ','קוטקס','תחבושות','שקמה','ליסטרין',
                             'אלביב','אלסבון','סבון ידיים','סבון מוצק','סבון פלמוליב',
                             'אקוה פרש','וולה ','ספריי לשיער','דאב ',
                             'בדין ','קיסם ',
                             'קר.לחות','תח. גוף','ספריי חזק לשיער','ספריי סאיוס','מרכך ש.','או בה ','וניש גל','ת.פנים']): return 'hygiene'
    # כלי בית
    if any(w in n for w in ['נייר טואלט','מגבות נייר','נייר אפיי','ניילון נצמד',
                             'שקיות מזון','צלחת קרם','קעריות','מגש חד פעמי',
                             'כפית קרם 100','מזלג קרם 100','סכין קרם 100','צלחות קטנות קרם']): return 'household'
    # אלכוהול
    if any(w in n for w in ['וודקה','ויסקי','וויסקי','ליקר ','שמפניה','פרוסקו','בריזר',
                             'קמפארי','רום ','טקילה','בייליס','קוניאק',
                             'בלוגה','סמירנוף','פינלנדיה','גוני ווקר',
                             'ריזלינג','מרלו','קברנה','פינו ','ערק ',
                             'גין ','סיידר אלכוהולי']): return 'alcohol'
    if 'בירה ' in n and 'ללא אלכוהול' not in n: return 'alcohol'
    if 'יין ' in n and not any(w in n for w in ['מיונז','חומץ','רוטב','ממרח']): return 'alcohol'
    if any(w in n for w in ['בלו ווין','קוקטייל ',
                             'דון חוליו','סיידר תפוחים']): return 'alcohol'
    # חיות מחמד
    if any(w in n for w in ['פריסקיז','פדיגרי','ויסקס','פנסי פיסט','מזון לכלב',
                             'מזון לחתול','חול לחתול']): return 'pets'
    # קפוא
    if any(w in n for w in ['קפוא','מוקפא','גלידה','גלידל','שלגון','שרבט',
                             'ארטיק ','בורקס','עלי בצק','בצק עלים','כופתאות',
                             'פיצה ','פיצת','בסיס לפיצה','בצק פיצה','גלידונית',
                             'טבעות בצל','לקט ירקות','חגיגה ירוקה',
                             'מאגדת שלישיה','מאגדת שמיניה','מאגדת 4 שייק','מאגדת טעמקור','מאגדת קול','לה קרמריה']): return 'frozen'
    # עוף
    if any(w in n for w in ['עוף ','חזה עוף','שוקיים','כנפיים','פרגית','הודו ',
                             'שוקי עוף','כרעיים','עוף שלם','עוף טרי','כבד עוף',
                             'נקניקיות עוף','שניצל עוף','ירכי עוף','פילה עוף',
                             'כרעי עוף','גריל עוף','שווארמה עוף','קציצות עוף',
                             'פטה הודו']): return 'poultry'
    # בשר
    if any(w in n for w in ['בשר בקר','בשר טחון','סטייק','אנטריקוט','נקניק','סלמי',
                             'פסטרמה','קבב','המבורגר','נקניקיה','מרגז','בורגר',
                             'לשון','כתף בקר','שריר בקר','צלעות','קציצות','טלה ',
                             'שניצל דק','רוסטביף','קורנדביף','סרוולד',
                             'בשר גולש','בשר מפורק','נגיסי בקר',
                             'שניצל מתובל','שניצלונים','שניצל גונגל',
                             'שניצל מיני','מקלוני שניצל','שניצל תירס',
                             'כדורי בשר','שניצל צמחונ','בונזו בשר',
                             'אצבעות שניצל','שניצל פרימ','קממבר בקר','קובה ביתי','פסט\' במעשנה']): return 'meat'
    # דגים
    if any(w in n for w in ['טונה','סרדין','סלמון','הרינג','בקלה','דג ','דגים',
                             'פורל','בס ים','לוקוס','גפילטה','קוויאר','איקרה',
                             'נסיכת הנילוס','פילה נסיכ','פילה בורי','פילה לברק',
                             'אנשובי','פילה מטיאס','פילה מקרוסק']): return 'fish'
    # ביצים
    if any(w in n for w in ['ביצים','ביצה ','ביצת']): return 'eggs'
    # שוקולד (לפני dairy!)
    if any(w in n for w in ['שוקולד','שוק.','פסק זמן','סניקרס','באונטי','לינדט',
                             'קינדר','מילקה','טובלרון','קיטקט','טראפל','פרלין',
                             'מרסי','טופיפי','מלטיזרס','מרס ','טוויקס',
                             'בונבונ','בונבוניירה','שוקולית','ביאנקו פלוס',
                             'רבע לשבע','עד חצות','שלווה ']): return 'chocolate'
    # חלב ומוצרי חלב
    if any(w in n for w in ['חלב ','גבינ','גב.','יוגורט','יוגור.','שמנת',
                             'חמאה','מוצרלה','ריקוטה','קשקבל','קאשקבל','בולגרית',
                             'דנונה','יופלה','אירן ','אקטיביה','קוטג','שוקו ',
                             'לאבן','לאבנה','פרומז','ממרח גבינ','מסקרפונה',
                             'רויון','לבנה ','גאודה','פקורינו','צדר ',
                             'מק אנד ציז','טבורוג','מלבי','קצפת','קוואטרו',
                             'ירח מתוק','דנונ.','גוש חלב','פטה ',
                             'גב9%','גב.9','טל העמק']): return 'dairy'
    if 'לבן ' in n and any(c.isdigit() for c in n): return 'dairy'
    # ירקות
    veggie = ['עגבניה','מלפפון','גזר ','חסה ','פלפל ירוק','פלפל אדום','פלפל צהוב',
              'פלפל כתום','פלפל חריף','פלפל שישקה','פלפל רמירו','פלפל צומה',
              'פלפל פאלמרו','בצל ירוק','בצל יבש','בצל אדום','שאלוט',
              'תפוח אדמה','תפוחי אדמה','חציל','קישוא','קשוא','זוקיני',
              'כרוב ','ברוקולי','כרובית','תרד ','בטטה ','עגבניה תמר','עגבניה שרי',
              'פטרייה','פטריות','שמיר ','כוסברה ','קולורבי','שומר ',
              'סלרי ','דלורית','קארע ','סלק ','מארז עירית','מארז רוקט',
              'מארז בזיליקום','מארז תירס','מארז עגבנות','מארז פלפל',
              'שום יבוא']
    if any(w in n for w in veggie):
        if not any(ex in n for ex in ['ממרח','במיונז','מטוגן','רוטב','קפוא']): return 'produce'
    # פירות
    fruit = ['תפוח עץ','תפוחונים','אגס ','ענבים','ענב ','בננה ','לימון','תפוז',
             'אבוקדו','תות ','אשכולית','מנגו ','אבטיח','מלון ','קיווי ','רימון ',
             'דובדבן','אוכמניות']
    if any(w in n for w in fruit):
        if not any(ex in n for ex in ['מיץ','סירופ','שוופס','חליטה','רוטב','סבון','יבש','בפחית','אושן']): return 'produce'
    # שימורים
    if any(w in n for w in ['גרגירי חומוס','תירס קל','גרעיני תירס','עגבניות מרוסקות',
                             'עגבניות קצוצות','מלפפון חומץ','זית ','זיתים','חומוס',
                             'קורנישונ','עלי גפן','כבוש','כבושים',
                             'מרק ','נמס בכוס','שעועית','עדשים ירוקות',
                             'סלט חצילים','חריסה','מטבוחה','נודלס ','מנה חמה',
                             'מנת השף','כיסונים','תירס מתוק','תירס לייט',
                             'אפונת גינה','אפונה עדינה','ארטישוק תחתיות',
                             'תענוג פרי','מעדן פרי',
                             'סלט קולסלאו','מלפפונים חומץ','תערובת קניידל']): return 'pantry'
    # לחם ומאפים
    if any(w in n for w in ['לחם ','פיתה ','פיתות','בגט',' חלה','חלה ','לחמניה',
                             'לחמניית','לחמית','קרואסון','כיכר ','לאפה','טורטיה',
                             'טוסטעים','פת קלוי','פירורי מאפה','קרוטונים',
                             'מיני קרוטונ','מלווח ',
                             'בצק פריך','8 לחמניות']): return 'bakery'
    # עוגות
    if any(w in n for w in ['עוגה ','עוגת','עוגות','עוג.','בראוניז','מאפין',
                             'קאפקייק','רולדה','סנדוויץ','פתי בר','קרמוגית',
                             'מארז הפסקת אוכל','קצפיות מרנג']): return 'cakes'
    # עוגיות
    if any(w in n for w in ['עוגיה','עוגיות','ביסקוויט','ופלים','ופל ','קרקר ',
                             'פתית ','ערגליות','אוריאו','וופל ','מיני קרקר']): return 'cookies'
    # ממתקים
    if any(w in n for w in ['סוכרייה','סוכריות','סוכריה','מסטיק ','טיק טק','סקיטלס',
                             'לוקיטוס','חמצוץ','גומי ','מנטוס','אורביט',
                             'מרשמלו','מזרה ','מיני סושקה']): return 'candy'
    # חטיפים
    if any(w in n for w in ['דוריטוס','פרינגלס','תפוציפס','ציפס','חטיף ','חטיפ.',
                             'בייגלה','פריכיות','פריכונים','ציטוס','כיפלי','ביסלי',
                             'פופקורן','במבה','אפרופו','נייטשר וואלי',
                             'הוטפופ ','דגן תפוח','סיני מיניס',
                             'תפוצ\'יפס','תפוצי\'פס','ציפס ירקות']): return 'snacks'
    # אגוזים
    if any(w in n for w in ['שקדים','אגוזי','אגוז ','קשיו','פיסטוק','בוטנים',
                             'גרעיני חמנ','גרעין חמניה','גרעיני דלעת','גרעין דלעת',
                             'צנוברים','פקאן','מיקס אגוז','פולי סויה',
                             'נייטשר פרוטאין']): return 'nuts'
    # קפה
    if any(w in n for w in ['קפה ','עלית טורקי','קפה נמס','נספרסו','אספרסו',
                             'קפה טחון','מאסטר קפה','טסטרס',
                             'קפסולות מס','קפסולות תערובת','קפסולות קליה',
                             'קפסולות סופרימו','קפסולות מילאנו']): return 'coffee'
    # תה
    if any(w in n for w in ['תה ','חליטה','חליטת','ינשוף','ויסוצקי','ליפטון','פומפדור']): return 'tea'
    # משקאות
    if any(w in n for w in ['מיץ ','פריגת','ספרינג ','ספרייט','קולה ','פנטה',
                             'נביעות','מי עדן','מים מינרל','מים מוגז','מים ',
                             'שוופס','תפוזינה','סחוט ','משקה ','סודה ','ענבית',
                             'רד בול','ציזיקי','פפסי','סבן אפ','מי טוניק',
                             'מונסטר ','אלפרו','סירופ ','מרדסו',
                             'משקהGO','מיץ תפוגזר','ס.גלי','לף בלונד','אושן ספריי']): return 'beverages'
    # שמנים ורטבים
    if any(w in n for w in ['שמן זית','שמן קנולה','שמן חמנ','שמן ','רוטב ',
                             'קטשופ','מיונז','חרדל','חומץ ','פסטו','ממרח ',
                             'טחינה','חלווה','קרם קוקוס','תרכיז עגבנ',
                             'חלבה']): return 'condiments'
    # תבלינים
    if any(w in n for w in ['כורכום','פפריקה','כמון','קינמון','קארי','פלפל שחור',
                             'זעתר','בהרת','שום טחון','שום יבש','תבלין ','מלח ',
                             'אבקת מרק','אגוז מוסקט','זנגויל','עלי דפנה']): return 'spices'
    # דגנים
    if any(w in n for w in ['אורז ','פסטה ','ספגטי','מקרוני','קוסקוס','בורגול',
                             'קינואה','עדשים','שעועית ','קמח ','פתיתים','אטריות',
                             'חיטה','כוסמת','שיבולת שועל','קוואקר','סולת ',
                             'לזניה','קנלוני','תערובת קמח','אינסטנט פודינג']): return 'grains'
    # ארוחת בוקר
    if any(w in n for w in ['דגני בוקר','קורנפלקס','קורני ','גרנולה ','ממרח השחר',
                             'נוטלה','דבש ','ריבה ','קונפיטורה','חמאת בוטנ',
                             'תערובת ל','לוטוס','קראנץ','ממרח שקדים']): return 'breakfast'
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
    from datetime import date
    curr_date, curr = dates[-1], prices[dates[-1]]
    # Find the last date where price was DIFFERENT from current (meaningful change)
    prev_date, prev = None, None
    for d in reversed(dates[:-1]):
        if abs(prices[d] - curr) / curr > 0.005:  # more than 0.5% different
            prev_date, prev = d, prices[d]
            break
    # If no meaningful change found, use most recent prev for "stable" signal
    if prev is None:
        prev_date, prev = dates[-2], prices[dates[-2]]
        pct = None  # stable — no change to report
        days = (date.fromisoformat(curr_date)-date.fromisoformat(prev_date)).days
        return prev, pct, days
    raw_pct = round((curr-prev)/prev*100,1)
    pct = raw_pct if abs(raw_pct) >= 1 else None
    days = (date.fromisoformat(curr_date)-date.fromisoformat(prev_date)).days
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
                products, _ = scrape_shufersal(store['store_id'], service, folder_id)
            else:
                products, _ = scrape_citymarket(store['store_branch'])
            print(f'  {len(products):,} מוצרים')
            new, upd = update_history(history, store['id'], products)
            print(f'  {new} חדשים | {upd} עודכנו')
            sh = history.get(store['id'], {})
            # Combined history from all stores for cross-store avg
            combined_sh = {}
            for sid, sdata in history.items():
                for bc2, hdata in sdata.items():
                    if bc2 not in combined_sh:
                        combined_sh[bc2] = dict(hdata)
                    elif hdata.get('pricePer100'):
                        combined_sh[bc2]['pricePer100'] = hdata['pricePer100']
            for p in products:
                bc = p['barcode']
                prev_price, pct, days = get_trend(bc, sh)
                avg, n = avg_similar(bc, combined_sh)
                vs_avg = round((p['pricePer100']-avg)/avg*100,1) if avg and p.get('pricePer100') else None
                # Price history stats for modal
                ph = sh.get(bc, {}).get('prices', {})
                ph_vals = list(ph.values()) if ph else []
                ph_dates = sorted(ph.keys()) if ph else []
                price_history = [{'d': d, 'p': ph[d]} for d in ph_dates]
                all_time_low  = min(ph_vals) if ph_vals else None
                all_time_high = max(ph_vals) if ph_vals else None
                hist_avg      = round(sum(ph_vals)/len(ph_vals), 2) if ph_vals else None
                p.update({'prevPrice':prev_price,'changePct':pct,'daysAgo':days,
                          'avgSimilarPer100':avg,'vsAvgPct':vs_avg,'nSimilar':n,
                          'priceHistory': price_history,
                          'allTimeLow': all_time_low, 'allTimeHigh': all_time_high,
                          'histAvg': hist_avg})
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
