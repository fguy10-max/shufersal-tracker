#!/usr/bin/env python3
"""
fetch_images.py — One-time image collector for מחירוסקופ
Reads shufersal_prices.json from GitHub, downloads product images,
and pushes them to the repo under imgs/{barcode}.jpg

Usage:
  GH_TOKEN=xxx python3 fetch_images.py

Run once to populate, then add --new flag for incremental updates:
  GH_TOKEN=xxx python3 fetch_images.py --new
"""

import os, sys, json, base64, time, requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Config ─────────────────────────────────────────────────────────
GH_TOKEN  = os.environ.get('GH_TOKEN')
GH_REPO   = 'fguy10-max/shufersal-tracker'
GH_API    = 'https://api.github.com'
PRICES_URL = f'https://raw.githubusercontent.com/{GH_REPO}/main/shufersal_prices.json'
IMG_FOLDER = 'imgs'
MAX_WORKERS = 15      # parallel downloads
SLEEP_BETWEEN_UPLOADS = 0.1  # seconds between GitHub API calls

# ── CDN fallback chain ──────────────────────────────────────────────
def image_urls(bc):
    urls = [f'https://d226b0iufwcjmj.cloudfront.net/global/sys/images/{bc}.jpg']
    if len(bc) == 13:
        p = f'{bc[0:3]}/{bc[3:6]}/{bc[6:9]}/{bc[9:]}'
        base = f'https://images.openfoodfacts.org/images/products/{p}'
        urls += [
            f'{base}/front_he.400.jpg',
            f'{base}/front_en.400.jpg',
            f'{base}/1.400.jpg',
        ]
    return urls

# ── GitHub helpers ──────────────────────────────────────────────────
session = requests.Session()
session.headers.update({
    'User-Agent': 'mchiroscop-imgbot/1.0',
    'Authorization': f'token {GH_TOKEN}',
})

def gh_file_exists(path):
    r = session.get(f'{GH_API}/repos/{GH_REPO}/contents/{path}', timeout=10)
    if r.status_code == 200:
        return r.json().get('sha')
    return None

def gh_upload_image(path, data_bytes, sha=None):
    payload = {
        'message': f'img: {path}',
        'content': base64.b64encode(data_bytes).decode(),
    }
    if sha:
        payload['sha'] = sha
    r = session.put(f'{GH_API}/repos/{GH_REPO}/contents/{path}',
                    json=payload, timeout=15)
    return r.status_code in (200, 201)

def get_existing_images():
    """Get set of barcodes already cached in imgs/ folder."""
    r = session.get(f'{GH_API}/repos/{GH_REPO}/contents/{IMG_FOLDER}', timeout=10)
    if r.status_code == 404:
        return set()
    if r.status_code == 200:
        files = r.json()
        return {f['name'].replace('.jpg','') for f in files if f['name'].endswith('.jpg')}
    return set()

# ── Image download ──────────────────────────────────────────────────
img_session = requests.Session()
img_session.headers.update({'User-Agent': 'Mozilla/5.0 (compatible; mchiroscop/1.0)'})

def download_image(bc):
    """Try each CDN URL in order, return (bc, image_bytes, source) or (bc, None, None)."""
    for url in image_urls(bc):
        try:
            r = img_session.get(url, timeout=3, stream=True)
            if r.status_code == 200:
                ct = r.headers.get('Content-Type', '')
                if 'image' in ct:
                    data = r.content
                    if len(data) > 1000:
                        source = 'CDN' if 'cloudfront' in url else 'OFF'
                        return bc, data, source
            elif r.status_code in (403, 404):
                continue  # fast fail, try next
        except requests.exceptions.Timeout:
            continue  # timeout, try next URL
        except Exception:
            continue
    return bc, None, None

# ── Main ────────────────────────────────────────────────────────────
def main():
    if not GH_TOKEN:
        print('❌ GH_TOKEN not set')
        sys.exit(1)

    incremental = '--new' in sys.argv

    print('מחירוסקופ — Image Fetcher')
    print('─' * 40)

    # Load prices JSON to get all barcodes
    print('📥 Loading prices JSON...')
    r = requests.get(PRICES_URL + '?t=' + str(int(time.time())), timeout=30)
    r.raise_for_status()
    data = r.json()

    # Collect unique barcodes across all stores
    barcodes = set()
    for store in data.get('stores', []):
        for cat, prods in store.get('categories', {}).items():
            for p in prods:
                bc = p.get('barcode', '')
                if bc and bc.isdigit():
                    barcodes.add(bc)

    print(f'📦 Found {len(barcodes):,} unique barcodes')

    # Check which already exist
    if incremental:
        print('🔍 Checking existing images...')
        existing = get_existing_images()
        barcodes = barcodes - existing
        print(f'⏭️  Skipping {len(existing)} already cached, {len(barcodes)} to fetch')
    else:
        print('🔄 Full run — fetching all barcodes')

    if not barcodes:
        print('✅ Nothing to do!')
        return

    barcodes = list(barcodes)
    print(f'\n🖼️  Downloading {len(barcodes)} images ({MAX_WORKERS} parallel)...\n')

    ok = fail = skipped = 0
    cdn_count = off_count = 0

    # Download in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(download_image, bc): bc for bc in barcodes}
        for i, fut in enumerate(as_completed(futures)):
            bc, img_data, source = fut.result()

            if img_data:
                # Upload to GitHub
                path = f'{IMG_FOLDER}/{bc}.jpg'
                sha = None  # full mode: always overwrite, no per-file check needed
                success = gh_upload_image(path, img_data, sha)
                if success:
                    ok += 1
                    if source == 'CDN': cdn_count += 1
                    else: off_count += 1
                else:
                    fail += 1
                time.sleep(SLEEP_BETWEEN_UPLOADS)
            else:
                skipped += 1

            # Progress
            done = i + 1
            if done % 50 == 0 or done == len(barcodes):
                pct = round(done / len(barcodes) * 100)
                print(f'  [{pct:3d}%] {done}/{len(barcodes)} — '
                      f'✅ {ok} uploaded ({cdn_count} CDN + {off_count} OFF) | '
                      f'❌ {skipped} not found | ⚠️ {fail} upload errors')

    print(f'\n{"─"*40}')
    print(f'סיום!')
    print(f'  ✅ Uploaded:   {ok} ({round(ok/len(barcodes)*100)}%)')
    print(f'     ├ CDN:      {cdn_count}')
    print(f'     └ OFF:      {off_count}')
    print(f'  ❌ Not found:  {skipped} ({round(skipped/len(barcodes)*100)}%)')
    print(f'  ⚠️  Errors:    {fail}')
    print(f'{"─"*40}')
    print(f'\nImages saved to: github.com/{GH_REPO}/tree/main/{IMG_FOLDER}/')
    print(f'Access via:      https://raw.githubusercontent.com/{GH_REPO}/main/{IMG_FOLDER}/{{barcode}}.jpg')

if __name__ == '__main__':
    main()
