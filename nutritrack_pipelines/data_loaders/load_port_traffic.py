import pandas as pd
import requests

@data_loader
def load_data(*args, **kwargs):
    url = 'https://api.worldbank.org/v2/country/all/indicator/IS.SHP.GOOD.TU'
    rows = []
    page = 1

    while page <= 50:
        r = requests.get(url, params={'format': 'json', 'per_page': 1000, 'page': page}, timeout=30)
        r.raise_for_status()
        body = r.json()

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