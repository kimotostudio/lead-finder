import csv
from pathlib import Path

inp = Path('output/leads_kawasaki.csv')
out = Path('output/kawasaki_list.csv')

rows = []
with inp.open('r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for r in reader:
        city = (r.get('city_guess') or '')
        # match if '川崎' appears anywhere in city guess or in name
        if '川崎' in city or '川崎' in (r.get('name') or ''):
            rows.append([r.get('name','').strip(), r.get('grade','').strip(), r.get('url','').strip()])
        if len(rows) >= 30:
            break

out.parent.mkdir(parents=True, exist_ok=True)
with out.open('w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['店舗名','判定','url'])
    writer.writerows(rows)

print(f'Wrote {len(rows)} rows to {out}')
