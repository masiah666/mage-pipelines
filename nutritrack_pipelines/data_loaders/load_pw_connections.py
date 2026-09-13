import pandas as pd
import requests

BASE = ('https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/'
        'spillovers_port_level_impact/FeatureServer/0/query')

@data_loader
def load_data(*args, **kwargs):
    rows, offset = [], 0
    while offset < 300000:                    # hard stop above the ~227k expected
        r = requests.get(BASE, params={
            'where': '1=1',
            'outFields': '*',
            'f': 'json',
            'resultOffset': offset,
            'resultRecordCount': 1000,
        }, timeout=60)
        r.raise_for_status()
        body = r.json()
        feats = body.get('features', [])
        rows.extend(f['attributes'] for f in feats)
        if offset % 20000 == 0:
            print(f"offset {offset}: {len(rows)} total so far")
        if not body.get('exceededTransferLimit'):
            break
        offset += len(feats)

    df = pd.DataFrame(rows)
    print(f"total: {len(df)}")
    print(df.columns.tolist())
    return df