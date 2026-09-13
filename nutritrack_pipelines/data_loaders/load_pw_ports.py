import pandas as pd
import requests

BASE = ('https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/'
        'PortWatch_ports_database/FeatureServer/0/query')

@data_loader
def load_data(*args, **kwargs):
    rows, offset = [], 0
    while offset < 10000:                      # hard stop
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
        print(f"offset {offset}: {len(feats)} rows")
        if not body.get('exceededTransferLimit'):
            break
        offset += len(feats)

    df = pd.DataFrame(rows)
    print(f"total: {len(df)}")
    print(df.columns.tolist())
    return df