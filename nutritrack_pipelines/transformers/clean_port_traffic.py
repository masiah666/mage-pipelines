import pandas as pd

@transformer
def transform(traffic, countries, *args, **kwargs):
    before = len(traffic)

    t = traffic[[
        'countryiso3code', 'country.id', 'country.value', 'date', 'value',
    ]].copy()
    t.columns = ['country_iso3', 'country_id', 'country_name', 'traffic_year', 'teu']

    t = t[t['teu'].notna()]
    t['traffic_year'] = t['traffic_year'].astype(int)
    t['teu'] = t['teu'].astype('int64')

    real = countries.loc[~countries['is_aggregate'], 'country_iso3']
    out = t[t['country_iso3'].isin(real)].copy()

    print(f"rows in: {before}")
    print(f"after dropping nulls: {len(t)}")
    print(f"after dropping aggregates: {len(out)}")
    print(f"countries: {out['country_iso3'].nunique()}, years: {out['traffic_year'].min()}-{out['traffic_year'].max()}")

    return out