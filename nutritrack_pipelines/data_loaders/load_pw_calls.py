import json
import time
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

BASE = ('https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/'
        'Daily_Ports_Data/FeatureServer/0/query')

VESSEL_TYPES = ['container', 'dry_bulk', 'general_cargo', 'roro', 'tanker']

CALL_STATS = json.dumps([
    {'statisticType': 'sum', 'onStatisticField': f'portcalls_{t}',
     'outStatisticFieldName': t}
    for t in VESSEL_TYPES
] + [
    {'statisticType': 'sum', 'onStatisticField': 'portcalls',
     'outStatisticFieldName': 'total'},
], separators=(',', ':'))


def fetch_json(params, attempts=6):
    last = None
    for attempt in range(attempts):
        try:
            r = requests.post(BASE, data=params, timeout=120)
            r.raise_for_status()
            body = r.json()
            if 'error' in body:
                raise RuntimeError(f"ArcGIS error: {body['error']}")
            return body
        except Exception as exc:
            last = exc
            if attempt < attempts - 1:
                delay = min(5 * 2 ** attempt, 80)
                print(f"  retry in {delay}s: {exc}")
                time.sleep(delay)
    raise RuntimeError(f"query failed after {attempts} attempts: {last}")


@data_loader
def load_data(*args, **kwargs):
    stats = json.dumps([{'statisticType': 'max', 'onStatisticField': 'year',
                         'outStatisticFieldName': 'y'}], separators=(',', ':'))
    body = fetch_json({'where': '1=1', 'outStatistics': stats, 'f': 'json'})
    max_year = int(body['features'][0]['attributes']['y'])
    years = [max_year - 1, max_year]
    print(f"layer max year {max_year}; fetching {years}")

    frames = []
    for year in years:
        offset, rows = 0, []
        while offset < 30000:
            body = fetch_json({
                'where': f'year={year}',
                'groupByFieldsForStatistics': 'portid,year,month',
                'outStatistics': CALL_STATS,
                'orderByFields': 'portid,month',
                'f': 'json',
                'resultOffset': offset,
                'resultRecordCount': 1000,
            })
            feats = body.get('features', [])
            rows.extend(f['attributes'] for f in feats)
            if not body.get('exceededTransferLimit'):
                break
            offset += len(feats)
        print(f"{year}: {len(rows)} port-months")
        frames.append(pd.DataFrame(rows))

    df = pd.concat(frames, ignore_index=True)
    print(f"total: {len(df)}")
    print(df.columns.tolist())
    return df
