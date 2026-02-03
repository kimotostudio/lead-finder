import os
import logging
from typing import List, Set

logger = logging.getLogger(__name__)

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    GOOGLE_AVAILABLE = True
except Exception:
    GOOGLE_AVAILABLE = False


DEFAULT_HEADER = [
    'store_name', 'url', 'comment', 'score', 'region', 'city', 'business_type',
    'site_type', 'phone', 'email', 'source_query', 'fetched_at_iso'
]


def _normalize_url(url: str) -> str:
    if not url:
        return ''
    url = url.strip()
    # ensure scheme
    if url.startswith('//'):
        url = 'https:' + url
    if not url.startswith('http://') and not url.startswith('https://'):
        url = 'https://' + url

    # remove fragment
    if '#' in url:
        url = url.split('#', 1)[0]

    # remove tracking params
    from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
    p = urlparse(url)
    qs = parse_qsl(p.query, keep_blank_values=True)
    qs = [(k, v) for (k, v) in qs if not (k.startswith('utm_') or k in ('fbclid', 'gclid'))]
    new_q = urlencode(qs)
    # strip trailing slash except root
    path = p.path
    if path.endswith('/') and path != '/':
        path = path.rstrip('/')

    new = urlunparse((p.scheme, p.netloc, path, p.params, new_q, ''))
    return new


def _get_service():
    if not GOOGLE_AVAILABLE:
        raise RuntimeError('google-api-python-client and google-auth are required')
    cred_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not cred_path:
        raise RuntimeError('GOOGLE_APPLICATION_CREDENTIALS not set')
    creds = service_account.Credentials.from_service_account_file(cred_path, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    service = build('sheets', 'v4', credentials=creds)
    return service


def ensure_sheet_and_header(spreadsheet_id: str, sheet_name: str, header: List[str] = None):
    """Ensure sheet exists and header row matches or is written."""
    header = header or DEFAULT_HEADER
    service = _get_service()
    sh = service.spreadsheets()

    # get metadata
    meta = sh.get(spreadsheetId=spreadsheet_id).execute()
    sheets = [s['properties']['title'] for s in meta.get('sheets', [])]

    if sheet_name not in sheets:
        # create sheet
        logger.info(f'Creating sheet: {sheet_name}')
        requests = [{
            'addSheet': {'properties': {'title': sheet_name}}
        }]
        sh.batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': requests}).execute()

    # read first row
    range_name = f"{sheet_name}!A1:Z1"
    resp = sh.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = resp.get('values', [])
    if not values or len(values[0]) == 0:
        # write header
        logger.info(f'Writing header to {sheet_name}')
        sh.values().update(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption='RAW', body={'values': [header]}).execute()


def read_existing_urls(spreadsheet_id: str, sheet_name: str) -> Set[str]:
    """Return set of normalized URLs from column B of the sheet."""
    service = _get_service()
    sh = service.spreadsheets()
    range_name = f"{sheet_name}!B2:B"
    resp = sh.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = resp.get('values', [])
    s = set()
    for row in values:
        if row:
            norm = _normalize_url(row[0])
            if norm:
                s.add(norm)
    return s


def append_rows(spreadsheet_id: str, sheet_name: str, rows: List[List[str]]):
    """Append rows (list of lists) to sheet_name. Assumes header exists."""
    service = _get_service()
    sh = service.spreadsheets()
    range_name = f"{sheet_name}!A:A"
    body = {'values': rows}
    resp = sh.values().append(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption='RAW', insertDataOption='INSERT_ROWS', body=body).execute()
    return resp
