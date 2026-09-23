"""Shared HTTP fetch with patient retries and a hard per-attempt deadline.

Remote APIs fail transiently: slow responses, bare 400s during a source's
republish window, and -- witnessed once from the World Bank -- connections
that stall open, trickling forever without erroring. requests' timeout only
bounds the connect and byte-gaps, not the total call, so each attempt also
runs under a hard wall-clock cap; a stalled attempt is abandoned and retried.

method: 'get' by default. ArcGIS grouped queries need 'post' (form data) --
GET encoding of their JSON parameters draws bare 400s.
"""
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

import requests


def _once(url, params, timeout, method):
    if method == 'post':
        r = requests.post(url, data=params, timeout=timeout)
    else:
        r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    body = r.json()
    if isinstance(body, dict) and 'error' in body:
        raise RuntimeError(f"API error: {body['error']}")
    return body


def fetch_json(url, params, attempts=6, timeout=60, hard_cap=90, method='get'):
    last = None
    for attempt in range(attempts):
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(_once, url, params, timeout, method)
                return future.result(timeout=hard_cap)
        except FutureTimeout:
            last = RuntimeError(f"attempt exceeded {hard_cap}s hard cap (stalled connection)")
        except Exception as exc:
            last = exc
        if attempt < attempts - 1:
            delay = min(5 * 2 ** attempt, 80)
            print(f"  retry in {delay}s: {last}")
            time.sleep(delay)
    raise RuntimeError(f"query failed after {attempts} attempts: {last}")