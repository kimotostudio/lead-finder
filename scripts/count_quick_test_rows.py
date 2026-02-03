from pathlib import Path
p = Path('output/quick_test.csv')
if not p.exists():
    print('0')
else:
    with p.open('r', encoding='utf-8') as f:
        n = sum(1 for _ in f) - 1
    print(n)
