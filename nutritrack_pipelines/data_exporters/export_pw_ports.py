from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from nutritrack_pipelines.utils.quality import (
    run_checks, has_rows, no_duplicate_keys, no_nulls,
)

ASSET_KEY = 'port_raw.portwatch_ports'

RENAME = {
    'portid': 'portid',
    'portname': 'portname',
    'country': 'country',
    'ISO3': 'country_iso3',
    'continent': 'continent',
    'lat': 'latitude',
    'lon': 'longitude',
    'LOCODE': 'locode',
    'vessel_count_total': 'vessel_count_total',
}


def sql_str(v):
    if v is None or v != v:
        return 'NULL'
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


@data_exporter
def export_data(df: DataFrame, **kwargs) -> None:
    out = df[list(RENAME)].rename(columns=RENAME)

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:

        for _, row in out.iterrows():
            cols = ', '.join(out.columns)
            vals = ', '.join(sql_str(v) for v in row)
            updates = ', '.join(
                f"{c} = EXCLUDED.{c}" for c in out.columns if c != 'portid'
            )
            loader.execute(f"""
                INSERT INTO port_raw.portwatch_ports ({cols})
                VALUES ({vals})
                ON CONFLICT (portid) DO UPDATE SET {updates};
            """)

        loader.execute(f"""
            INSERT INTO registry.assets (
                asset_key, schema_name, table_name, layer, description,
                source_system, source_detail, grain, owner,
                pipeline_uuid, block_uuid, last_run_at, last_row_count
            ) VALUES (
                '{ASSET_KEY}', 'port_raw', 'portwatch_ports', 'raw',
                'PortWatch ports reference with real port coordinates',
                'IMF PortWatch',
                'PortWatch_ports_database',
                'one row per port',
                'Haisam',
                '{kwargs.get("pipeline_uuid")}',
                '{kwargs.get("block_uuid")}',
                now(), {len(out)}
            )
                        ON CONFLICT (asset_key) DO UPDATE SET
                last_run_at    = EXCLUDED.last_run_at,
                last_row_count = EXCLUDED.last_row_count,
                pipeline_uuid  = EXCLUDED.pipeline_uuid,
                block_uuid     = EXCLUDED.block_uuid;
        """)

        run_checks(loader, ASSET_KEY, out, [
            ('has_rows', has_rows),
            ('unique_port', no_duplicate_keys('portid')),
            ('key_fields_present', no_nulls('portid', 'portname')),
        ])

        loader.conn.commit()
        print(f"upserted {len(out)} rows into {ASSET_KEY}")