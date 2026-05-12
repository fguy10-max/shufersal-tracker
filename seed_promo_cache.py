"""
One-time script to seed PromoFull cache in Drive.
Run locally or add temporarily to GitHub Actions.
"""
import requests, gzip, xml.etree.ElementTree as ET
import json, os, io
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from bs4 import BeautifulSoup

TODAY = datetime.now().strftime('%Y-%m-%d')
SHUFERSAL_BASE = 'https://prices.shufersal.co.il'
DRIVE_FOLDER = 'מחירוסקופ'
SCOPES = ['https://www.googleapis.com/auth/drive']

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})

# Drive
creds = service_account.Credentials.from_service_account_info(
    json.loads(os.environ['GDRIVE_CREDENTIALS']), scopes=SCOPES)
service = build('drive', 'v3', credentials=creds)
q = f"name='{DRIVE_FOLDER}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
folder_id = service.files().list(q=q, fields='files(id)').execute()['files'][0]['id']

def extract_promos(root):
    promos = {}
    for promo in root.iter('Promotion'):
        desc      = promo.findtext('PromotionDescription') or ''
        end       = promo.findtext('PromotionEndDateTime') or promo.findtext('PromotionEndDate') or ''
        is_coupon = promo.findtext('AdditionalIsCoupon') or '0'
        club_id   = promo.findtext('ClubID') or ''
        club_num  = club_id.strip().split(' ')[0]
        if end and end[:10] < TODAY: continue
        if club_num and club_num != '0': continue
        if 'פיצוי' in desc: continue
        label = f'🎫 קופון: {desc}' if is_coupon == '1' else desc
        for pitem in promo.iter('PromotionItem'):
            code = pitem.findtext('ItemCode') or ''
            pp   = pitem.findtext('DiscountedPrice') or ''
            if code and code != '0000000000000':
                promos[code] = {'promo': label, 'promoPrice': float(pp) if pp else None}
    return promos

def save_to_drive(filename, data):
    content = json.dumps(data, ensure_ascii=False).encode('utf-8')
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype='application/json')
    q = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    ex = service.files().list(q=q, fields='files(id)').execute()
    if ex['files']:
        service.files().update(fileId=ex['files'][0]['id'], media_body=media).execute()
    else:
        service.files().create(body={'name':filename,'parents':[folder_id]},
                               media_body=media, fields='id').execute()
    print(f"Saved {filename}: {len(data):,} promos")

for store_id in [287, 599]:
    print(f"\nProcessing store {store_id}...")
    # Get all files including PromoFull
    r = session.get(f'{SHUFERSAL_BASE}/FileObject/UpdateCategory',
                    params={'catID': 3, 'storeId': store_id, 'sort': 1, 'order': 1})
    soup = BeautifulSoup(r.text, 'html.parser')
    
    for row in soup.find_all('tr'):
        row_text = row.get_text()
        if 'promofull' not in row_text.lower(): continue
        a = row.find('a', href=True)
        if not a: continue
        href = a['href']
        if not href.startswith('http'): href = SHUFERSAL_BASE + href
        
        print(f"  Downloading PromoFull...")
        r2 = session.get(href, timeout=120)
        content = gzip.decompress(r2.content)
        root = ET.fromstring(content.decode('utf-8', errors='replace'))
        promos = extract_promos(root)
        save_to_drive(f'promo_cache_{store_id}.json', promos)
        break

print("\nDone!")
