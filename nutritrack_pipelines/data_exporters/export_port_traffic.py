from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


ASSET_KEY = 'port_raw.country_port_traffic'

FIELD_DOCS = {
    'country_iso3':  ('ISO 3166-1 alpha-3 country code', None),
    'country_id':    ('World Bank two-character entity code', None),
    'country_name':  ('Country display name', None),
    'traffic_year':  ('Calendar year of observation', 'year'),
    'teu':           ('Container port traffic', 'TEU'),
}


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    schema_name = 'port_raw'
    table_name = 'country_port_traffic'
    config_path = path.join(get_repo_path(), 'io_config.yaml')

    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        loader.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name};')
        loader.export(df, schema_name, table_name, index=False, if_exists='replace')

        loader.execute(f"""
            INSERT INTO registry.assets (
                asset_key, schema_name, table_name, layer, description,
                source_system, source_detail, grain, owner,
                pipeline_uuid, block_uuid, last_run_at, last_row_count
            ) VALUES (
                '{ASSET_KEY}', '{schema_name}', '{table_name}', 'raw',
                'Annual container port traffic in TEU by country',
                'World Bank Indicators API',
                'IS.SHP.GOOD.TU',
                'one row per country per year',
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

        loader.conn.commit()

        print(f"exported {len(df)} rows and registered {ASSET_KEY}")