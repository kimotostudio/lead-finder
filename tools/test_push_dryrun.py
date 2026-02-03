"""Simple dry-run test for push_to_sheets.
Creates a tiny CSV and runs push_to_sheets in dry-run mode.
"""
import csv
import os
import subprocess

csv_path = os.path.join('tools', 'test_sample.csv')
with open(csv_path, 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['store_name','url','comment','score','region','city','business_type','site_type','phone','email','source_query','fetched_at_iso'])
    writer.writerow(['Test Salon','https://example.com/test','△ minor SEO','75','tokyo','Shinjuku','Counseling','brochure','03-0000-0000','info@example.com','query1','2026-01-18T00:00:00'])

print('Running dry-run...')
subprocess.run(['python', 'tools/push_to_sheets.py', '--region', 'tokyo', '--csv', csv_path, '--dry-run'], check=False)
