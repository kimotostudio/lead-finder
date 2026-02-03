import requests
import time
import sys

BASE='http://127.0.0.1:5000'

payload={
    "prefecture":"東京都",
    "cities":["新宿区"],
    "business_types":["ヒーリング"],
    "limit":3
}

try:
    r = requests.post(BASE + '/api/search', json=payload, timeout=10)
except Exception as e:
    print('POST failed:', e)
    sys.exit(2)

print('POST', r.status_code, r.text)
if not r.ok:
    sys.exit(1)

final = None
for i in range(120):
    try:
        p = requests.get(BASE + '/api/progress', timeout=5).json()
    except Exception as e:
        print('Poll failed:', e)
        time.sleep(1)
        continue
    print(i, p)
    if p.get('status') in ('done', 'error'):
        final = p
        break
    time.sleep(1)

print('\nFinal:', final)
if final and final.get('status') == 'error':
    sys.exit(3)

sys.exit(0)
