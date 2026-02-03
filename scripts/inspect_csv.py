import sys
from pathlib import Path

if len(sys.argv) < 2:
    print('Usage: inspect_csv.py <csv_path>')
    sys.exit(2)

p = Path(sys.argv[1])
if not p.exists():
    print('File not found:', p)
    sys.exit(1)

# Read first bytes
with open(p, 'rb') as f:
    sample = f.read(4)
    print('First bytes:', sample)

# Read header lines with utf-8-sig
with open(p, 'r', encoding='utf-8-sig', newline='') as f:
    lines = [next(f).rstrip('\n')]
    try:
        lines.append(next(f).rstrip('\n'))
    except StopIteration:
        pass
    print('\nDecoded lines:')
    for i, L in enumerate(lines, start=1):
        print(f'{i}: {L}')

# Print parsed header columns
import csv
with open(p, 'r', encoding='utf-8-sig', newline='') as f:
    reader = csv.reader(f)
    header = next(reader)
    print('\nHeader columns count:', len(header))
    print('Header columns sample:', header[:10])

print('\nDone')
