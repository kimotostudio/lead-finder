import csv
import sys
from pathlib import Path
from urllib.parse import urlparse

# Ensure project root is in sys.path so we can import modules from parent folder
sys.path.append(str(Path(__file__).resolve().parents[1]))
from searcher import search_urls_for_query

queries_file = Path('data/queries_shinjuku.txt')
out_file = Path('output/shinjuku_list.csv')

# Load queries
queries = []
with queries_file.open('r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#'):
            queries.append(line)

seen = set()
rows = []
for q in queries:
    urls = search_urls_for_query(q, max_results=15)
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        parsed = urlparse(u)
        name = parsed.netloc
        rows.append([name, '', u])
        if len(rows) >= 40:
            break
    if len(rows) >= 40:
        break

out_file.parent.mkdir(parents=True, exist_ok=True)
with out_file.open('w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['店舗名','判定','url'])
    writer.writerows(rows)

print(f'Wrote {len(rows)} rows to {out_file}')
