from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


ASSET_KEY = 'port_mart.country_traffic_summary'

FIELD_DOCS = {
    'country_iso3':    ('ISO 3166-1 alpha-3 code', None),
    'country_name':    ('Country display name', None),
    'region_code':     ('World Bank region code', None),
    'region_name':     ('World Bank region name', None),
    'longitude':       ('Capital city longitude, NOT port location', 'degrees'),
    'latitude':        ('Capital city latitude, NOT port location', 'degrees'),
    'latest_year':     ('Most recent year with a reported figure', 'year'),
    'latest_teu':      ('TEU in latest_year', 'TEU'),
    'teu_2019':        ('TEU in 2019, the last year with full global coverage', 'TEU'),
    'teu_2010':        ('TEU in 2010, the growth baseline', 'TEU'),
    'cagr_2010_2019':  ('Compound annual growth rate 2010-2019', 'fraction'),
    'rank_2019':       ('Global rank by 2019 TEU, 1 = largest', None),
}


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    schema_name = 'port_mart'
    table_name = 'country_traffic_summary'
    config_path = path.join(get_repo_path(), 'io_config.yaml')

    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        loader.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name};')
        loader.execute(f'DROP TABLE IF EXISTS {schema_name}.{table_name};')
        loader.export(df, schema_name, table_name, index=False, if_exists='replace')

        loader.execute(f"""
            INSERT INTO registry.assets (
                asset_key, schema_name, table_name, layer, description,
                source_system, source_detail, grain, owner,
                pipeline_uuid, block_uuid, last_run_at, last_row_count
            ) VALUES (
                '{ASSET_KEY}', '{schema_name}', '{table_name}', 'mart',
                'Per-country container traffic summary with growth and ranking',
                'Derived',
                'Built from port_raw.country_port_traffic and port_raw.country_dim',
                'one row per country',
                'Haisam',
                '{kwargs.get("pipeline_uuid")}',
                '{kwargs.get("block_uuid")}',
                now(), {len(df)}
            )
            ON CONFLICT (asset_key) DO UPDATE SET
                last_run_at    = EXCLUDED.last_run_at,
                last_row_count = EXCLUDED.last_row_count;
        """)

        for col in df.columns:
            desc, unit = FIELD_DOCS.get(col, (None, None))
            desc_sql = f"'{desc}'" if desc else 'NULL'
            unit_sql = f"'{unit}'" if unit else 'NULL'
            loader.execute(f"""
                INSERT INTO registry.fields
                    (asset_key, field_name, data_type, description, unit, is_nullable)
                VALUES
                    ('{ASSET_KEY}', '{col}', '{df[col].dtype}',
                     {desc_sql}, {unit_sql}, {bool(df[col].isna().any())})
                ON CONFLICT (asset_key, field_name) DO UPDATE SET
                    data_type   = EXCLUDED.data_type,
                    is_nullable = EXCLUDED.is_nullable;
            """)

        for upstream in ['port_raw.country_port_traffic', 'port_raw.country_dim']:
            loader.execute(f"""
                INSERT INTO registry.edges (from_asset, to_asset, edge_type)
                VALUES ('{upstream}', '{ASSET_KEY}', 'feeds')
                ON CONFLICT DO NOTHING;
            """)

        loader.conn.commit()
        print(f"exported {len(df)} rows and registered {ASSET_KEY}")