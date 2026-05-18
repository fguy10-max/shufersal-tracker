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
    {'id':'goodpharm_givataim','name':'גוד פארם גבעתיים','short':'גוד פארם','source':'goodpharm','store_branch':'970'},
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
    if isinstance(text, bytes):
        text = text.decode('utf-8', errors='replace')
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
            name=g('ItemName') or g('item_name'); price_str=(g('ItemPrice') or g('Price')).strip()
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

def parse_promo_price(desc, regular_price):
    """Port of JS parsePromoPrice — returns effective unit price or None."""
    import re as _re
    if not desc or '🎫' in desc: return None
    if any(w in desc for w in ['מעל קניה','בקניה']): return None
    unit_price = None

    # Pattern 1: NבPRICE — "2ב22", "3ב18"
    m = _re.search(r'(\d+)\s*ב\s*(\d+(?:[.,]\d+)?)', desc)
    if m:
        qty = int(m.group(1))
        total = float(m.group(2).replace(',','.'))
        up = round(total / qty, 2)
        if 0 < up < regular_price: unit_price = up

    # Pattern 2: "ב2- ב 65"
    if not unit_price:
        m = _re.search(r'ב\s*\d+\s*[-–]\s*ב\s*(\d+(?:[.,]\d+)?)', desc)
        if m:
            up = float(m.group(1).replace(',','.'))
            if 0 < up < regular_price: unit_price = up

    # Pattern 3a: "ב79.90-"
    if not unit_price:
        m = _re.search(r'ב(\d+(?:[.,]\d+)?)\s*[-–]', desc)
        if m:
            up = float(m.group(1).replace(',','.'))
            if 0 < up < regular_price: unit_price = up

    # Pattern 3b: "ב- 149", "ב. 23.90"
    if not unit_price:
        m = _re.search(r'ב\s*[-–.]\s*(\d+(?:[.,]\d+)?)', desc)
        if m:
            up = float(m.group(1).replace(',','.'))
            if 0 < up < regular_price: unit_price = up

    # Pattern 4: 1+1
    if not unit_price and '1+1' in desc:
        unit_price = round(regular_price * 0.5, 2)

    # Pattern 5: standalone price
    if not unit_price:
        m = _re.search(r'(?:^|[\s,])(\d+(?:[.,]\d+)?)(?=\s|$)', desc)
        if m:
            up = float(m.group(1).replace(',','.'))
            if 0 < up < regular_price: unit_price = up

    return unit_price

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
                # BUG FIX: Strip whitespace before parsing string to float
                pp   = (pitem.findtext('DiscountedPrice') or '').strip()
                if code and code != '0000000000000':
                    promos[code] = {'promo': label, 'promoPrice': float(pp) if pp else None}
            # Format 2: <Item>
            for item in promo.iter('Item'):
                code = item.findtext('ItemCode') or item.findtext('Barcode') or ''
                pp   = (item.findtext('ItemPrice') or item.findtext('DiscountedPrice') or '').strip()
                if code and code != '0000000000000':
                    promos[code] = {'promo': label, 'promoPrice': float(pp) if pp else None}
            # Format 3: ItemCode directly in Promotion
            direct = promo.findtext('ItemCode')
            if direct and direct != '0000000000000':
                pp = (promo.findtext('DiscountedPrice') or '').strip()
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
                github_upload
