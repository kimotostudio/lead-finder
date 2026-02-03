import sys
from src.normalize import FINAL_SCHEMA

if len(sys.argv) < 2:
    print('Usage: fix_header.py <csv_path>')
    sys.exit(1)

p = sys.argv[1]
with open(p, 'r', encoding='utf-8-sig') as f:
    lines = f.readlines()
if not lines:
    print('Empty file:', p)
    sys.exit(1)

lines[0] = ','.join(FINAL_SCHEMA) + '\n'
with open(p, 'w', encoding='utf-8-sig', newline='') as f:
    f.writelines(lines)

print('Rewrote header for', p)
