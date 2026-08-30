import pandas as pd
import requests

@data_loader
def load_data(*args, **kwargs):
    r = requests.get(
        'https://api.worldbank.org/v2/country',
        params={'format': 'json', 'per_page': 400},
        timeout=30,
    )
    r.raise_for_status()
    df = pd.json_normalize(r.json()[1])

    out = df[[
        'id', 'iso2Code', 'name',
        'region.id', 'region.value',
        'capitalCity', 'longitude', 'latitude',
    ]].copy()

    out.columns = [
        'country_iso3', 'country_iso2', 'country_name',
        'region_code', 'region_name',
        'capital_city', 'longitude', 'latitude',
    ]
    out['longitude'] = pd.to_numeric(out['longitude'], errors='coerce')
    out['latitude'] = pd.to_numeric(out['latitude'], errors='coerce')
    out['is_aggregate'] = out['region_name'] == 'Aggregates'

    print(f"rows: {len(out)}  aggregates: {out['is_aggregate'].sum()}")
    return out