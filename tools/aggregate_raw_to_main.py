#!/usr/bin/env python3
"""
Aggregate *_raw sheets into Main using Google Sheets API.
Matches the Apps Script logic: select rows with score>=40, extract A-E,
dedupe by URL (keep highest score), preserve existing IDs in Main,
assign new IDs starting at 3000 (formatted as 5 digits, e.g. 03000).

Usage: python tools/aggregate_raw_to_main.py
Environment: set GOOGLE_APPLICATION_CREDENTIALS and SHEETS_SPREADSHEET_ID
"""
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import sheets_writer


def normalize(s):
    if s is None:
        return ''
    return str(s).strip()


def main():
    spreadsheet_id = os.environ.get('SHEETS_SPREADSHEET_ID')
    if not spreadsheet_id:
        raise SystemExit('Set SHEETS_SPREADSHEET_ID in env')

    service = sheets_writer._get_service()
    sh = service.spreadsheets()

    meta = sh.get(spreadsheetId=spreadsheet_id).execute()
    sheets = meta.get('sheets', [])
    raw_sheet_names = [s['properties']['title'] for s in sheets if s['properties']['title'].lower().endswith('_raw')]

    if not raw_sheet_names:
        print('No _raw sheets found.')
        return

    # Load existing Main IDs
    main_name = 'Main'
    existing_id_map = {}
    existing_max = 0
    main_sheet = None
    for s in sheets:
        if s['properties']['title'] == main_name:
            main_sheet = s
            break

    if main_sheet:
        # read header and data
        # need to know last column count
        resp = sh.values().get(spreadsheetId=spreadsheet_id, range=f"{main_name}!1:1").execute()
        header = resp.get('values', [[]])[0]
        # find url column index
        url_idx = None
        for i, h in enumerate(header):
            if str(h).lower() == 'url':
                url_idx = i
                break
        # read data rows
        resp = sh.values().get(spreadsheetId=spreadsheet_id, range=f"{main_name}!A2:Z").execute()
        values = resp.get('values', [])
        for row in values:
            if not row:
                continue
            idval = row[0] if len(row) > 0 else ''
            urlval = row[url_idx] if (url_idx is not None and len(row) > url_idx) else (row[1] if len(row) > 1 else '')
            u = normalize(urlval)
            if u:
                existing_id_map[u] = idval
                try:
                    n = int(idval)
                    if n > existing_max:
                        existing_max = n
                except Exception:
                    pass

    # Collect candidates from raw sheets
    collected = []  # dicts with keys: url, row (A-E), score
    processed = 0
    for name in raw_sheet_names:
        # read header to find score column
        resp = sh.values().get(spreadsheetId=spreadsheet_id, range=f"{name}!1:1").execute()
        header = resp.get('values', [[]])[0]
        score_idx = None
        for i, h in enumerate(header):
            if str(h).lower() == 'score':
                score_idx = i
                break
        if score_idx is None:
            score_idx = 3  # default to D
        # read data
        resp = sh.values().get(spreadsheetId=spreadsheet_id, range=f"{name}!A2:Z").execute()
        rows = resp.get('values', [])
        for r in rows:
            processed += 1
            score = 0
            if len(r) > score_idx:
                try:
                    score = int(str(r[score_idx]).strip())
                except Exception:
                    score = 0
            if score < 40:
                continue
            # extract A-E
            out = [normalize(r[i]) if i < len(r) else '' for i in range(5)]
            url = out[1]
            if not url:
                continue
            collected.append({'url': url, 'row': out, 'score': score})

    if not collected:
        print('No candidate rows with score>=40')
        return

    # Dedupe by normalized url keep highest score
    by_url = {}
    for idx, item in enumerate(collected):
        u = normalize(item['url'])
        if u not in by_url:
            item['first_idx'] = idx
            by_url[u] = item
        else:
            if item['score'] > by_url[u]['score']:
                item['first_idx'] = by_url[u].get('first_idx', idx)
                by_url[u] = item

    results = list(by_url.values())
    results.sort(key=lambda x: (-x['score'], x.get('first_idx', 0)))

    # Build rows with IDs
    next_id = existing_max + 1 if existing_max > 0 else 3000
    out_rows = []
    for item in results:
        u = normalize(item['url'])
        if u in existing_id_map and existing_id_map[u]:
            idv = existing_id_map[u]
        else:
            idv = str(next_id)
            next_id += 1
        if idv.isdigit():
            idv = idv.zfill(5)
        row = [idv] + item['row']
        out_rows.append(row)

    # Write Main: header + rows
    header = ['id','store_name','url','comment','score','region']
    # clear or create sheet
    resp = sh.get(spreadsheetId=spreadsheet_id, ranges=[], includeGridData=False).execute()
    sheet_titles = [s['properties']['title'] for s in resp.get('sheets',[])]
    if main_name in sheet_titles:
        # clear contents
        sh.values().clear(spreadsheetId=spreadsheet_id, range=main_name).execute()
    else:
        # add sheet
        requests = [{'addSheet': {'properties': {'title': main_name}}}]
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': requests}).execute()

    # write header
    sh.values().update(spreadsheetId=spreadsheet_id, range=f"{main_name}!A1:F1", valueInputOption='RAW', body={'values': [header]}).execute()
    # write rows
    sh.values().update(spreadsheetId=spreadsheet_id, range=f"{main_name}!A2:F{len(out_rows)+1}", valueInputOption='RAW', body={'values': out_rows}).execute()

    print(f'Done. Processed {processed} raw rows, candidates {len(collected)}, deduped {len(results)}, written {len(out_rows)} to {main_name}')


if __name__ == '__main__':
    main()
