import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from nutritrack_pipelines.utils.fetch import fetch_json

URL = 'https://api.worldbank.org/v2/country/all/indicator/IS.SHP.GOOD.TU'


@data_loader
def load_data(*args, **kwargs):
    rows = []
    page = 1

    while page <= 50:
        body = fetch_json(URL, {'format': 'json', 'per_page': 1000, 'page': page})

        meta, records = body[0], body[1]
        rows.extend(records)

        print(f"page {page} of {meta['pages']} — {len(records)} records")

        if page >= meta['pages']:
            break
        page += 1

    df = pd.json_normalize(rows)
    print(f"total rows: {len(df)}")
    print(df.columns.tolist())
    return df