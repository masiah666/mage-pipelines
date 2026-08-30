import pandas as pd

BASE_YEAR = 2010
COMPARE_YEAR = 2019

@transformer
def transform(df, *args, **kwargs):
    latest = (
        df.sort_values('traffic_year')
          .groupby('country_iso3')
          .tail(1)[['country_iso3', 'traffic_year', 'teu']]
          .rename(columns={'traffic_year': 'latest_year', 'teu': 'latest_teu'})
    )

    compare = (
        df[df['traffic_year'] == COMPARE_YEAR][['country_iso3', 'teu']]
          .rename(columns={'teu': 'teu_2019'})
    )

    base = (
        df[df['traffic_year'] == BASE_YEAR][['country_iso3', 'teu']]
          .rename(columns={'teu': 'teu_2010'})
    )

    dims = (
        df[['country_iso3', 'country_name', 'region_code',
            'region_name', 'longitude', 'latitude']]
          .drop_duplicates('country_iso3')
    )

    out = (
        dims.merge(latest, on='country_iso3', how='left')
            .merge(compare, on='country_iso3', how='left')
            .merge(base, on='country_iso3', how='left')
    )

    years = COMPARE_YEAR - BASE_YEAR
    out['cagr_2010_2019'] = (
        (out['teu_2019'] / out['teu_2010']) ** (1 / years) - 1
    ).round(4)

    out['rank_2019'] = out['teu_2019'].rank(ascending=False, method='min')

    print(f"countries: {len(out)}")
    print(f"with 2019 figure: {out['teu_2019'].notna().sum()}")
    print(f"with CAGR: {out['cagr_2010_2019'].notna().sum()}")
    return out