"""One-shot prod selftest via HTTP. Usage:
  python scripts/prod_ops_selftest.py https://ff-m1-... token
"""
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request


def main():
    base = (sys.argv[1] if len(sys.argv) > 1 else '').rstrip('/')
    token = sys.argv[2] if len(sys.argv) > 2 else ''
    if not base or not token:
        print('usage: prod_ops_selftest.py <base_url> <token>')
        return 2
    url = f'{base}/ops/api/selftest?token={urllib.parse.quote(token)}&cleanup=1'
    req = urllib.request.Request(url, method='POST', data=b'')
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode('utf-8', errors='replace')
            print(body)
            data = json.loads(body)
            return 0 if data.get('ok') else 1
    except urllib.error.HTTPError as e:
        print(e.read().decode('utf-8', errors='replace'))
        return 1
    except Exception as e:
        print('ERR', e)
        return 1


if __name__ == '__main__':
    import urllib.parse
    raise SystemExit(main())
