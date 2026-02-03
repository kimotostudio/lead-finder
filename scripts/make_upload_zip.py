import os
import zipfile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
OUT = os.path.join(ROOT, 'lead-finder_for_claude.zip')

EXCLUDE_DIRS = {
    '.venv', 'venv', 'logs', 'output', '__pycache__', '.git', 'site-generator/.venv',
}

def should_exclude(path):
    for ex in EXCLUDE_DIRS:
        if path.startswith(ex) or ('/' + ex + '/') in path.replace('\\','/'):
            return True
    return False

with zipfile.ZipFile(OUT, 'w', zipfile.ZIP_DEFLATED) as z:
    for dirpath, dirnames, filenames in os.walk(ROOT):
        rel = os.path.relpath(dirpath, ROOT)
        if rel == '.':
            rel = ''
        # skip excluded dirs
        parts = rel.split(os.sep) if rel else []
        if any(p in EXCLUDE_DIRS for p in parts):
            continue
        for f in filenames:
            # skip the output zip itself
            if f == os.path.basename(OUT):
                continue
            full = os.path.join(dirpath, f)
            arcname = os.path.join(rel, f) if rel else f
            z.write(full, arcname)

print('Created', OUT)
