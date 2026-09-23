import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from nutritrack_pipelines.utils.fetch import fetch_json

BASE = ('https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/'
        'PortWatch_ports_database/FeatureServer/0/query')


@data_loader
def load_data(*args, **kwargs):
    rows, offset = [], 0
    while offset < 10000:
        body = fetch_json(BASE, {
            'where': '1=1',
            'outFields': '*',
            'f': 'json',
            'resultOffset': offset,
            'resultRecordCount': 1000,
        }, method='post')
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