import csv
from pathlib import Path

inp = Path('output/leads_final.csv')
out = Path('output/shinjuku_list.csv')

rows = []
with inp.open('r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for r in reader:
        city = (r.get('city_guess') or '')
        name = (r.get('name') or '')
        if '新宿' in city or '新宿' in name:
            rows.append([name.strip(), r.get('grade','').strip(), r.get('url','').strip()])
        if len(rows) >= 40:
            break

out.parent.mkdir(parents=True, exist_ok=True)
with out.open('w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['店舗名','判定','url'])
    writer.writerows(rows)

print(f'Wrote {len(rows)} rows to {out}')
