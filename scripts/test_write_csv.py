from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.output_writer import OutputWriter

sample = [{
    'shop_name': 'テスト店',
    'url': 'https://example.com/test',
    'grade': 'A',
    'score': 85,
    'business_type': 'カウンセリング',
    'owner_name': '山田太郎',
    'phone': '03-1234-5678',
    'email': 't@example.com',
    'address': '東京都新宿区1-2-3',
    'city': '新宿区',
    'business_hours': '9:00-18:00',
    'domain': 'example.com',
    'site_type': 'wordpress',
    'reasons': 'no_booking; no_pricing'
}]

out = 'web_app/output/test_leads.csv'
OutputWriter.write_csv(sample, out)
print('Wrote', out)
