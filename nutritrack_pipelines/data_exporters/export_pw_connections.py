from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from nutritrack_pipelines.utils.quality import (
    run_checks, has_rows, no_duplicate_keys, no_nulls,
    non_negative, positive, no_self_legs,
)

ASSET_KEY = 'port_raw.port_connections'

COLS = ['from_portid', 'from_portname', 'from_country', 'from_iso3',
        'to_portid', 'to_portname', 'to_country', 'to_iso3',
        'average_transit_days', 'daily_capacity_at_risk',
        'relative_capacity_at_risk']

BATCH = 1000


def sql_str(v):
    if v is None or v != v:
        return 'NULL'
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


@data_exporter
def export_data(df: DataFrame, **kwargs) -> None:
    out = df[COLS]

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:

        loader.execute('DELETE FROM port_raw.port_connections;')

        col_list = ', '.join(COLS)
        for start in range(0, len(out), BATCH):
            chunk = out.iloc[start:start + BATCH]
            values = ',\n'.join(
                '(' + ', '.join(sql_str(v) for v in row) + ')'
                for row in chunk.itertuples(index=False)
            )
            loader.execute(
                f'INSERT INTO port_raw.port_connections ({col_list}) VALUES {values};'
            )

        loader.execute(f"""
            INSERT INTO registry.assets (
                asset_key, schema_name, table_name, layer, description,
                source_system, source_detail, grain, owner,
                pipeline_uuid, block_uuid, last_run_at, last_row_count
            ) VALUES (
                '{ASSET_KEY}', 'port_raw', 'port_connections', 'raw',
                'Directed origin-to-destination port pairs with observed sailing capacity',
                'IMF PortWatch',
                'spillovers_port_level_impact',
                'one row per directed port pair',
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
            ('unique_pair', no_duplicate_keys('from_portid', 'to_portid')),
            ('key_fields_present', no_nulls('from_portid', 'to_portid')),
            ('no_self_legs', no_self_legs),
            ('capacity_non_negative', non_negative('daily_capacity_at_risk')),
            ('transit_days_positive', positive('average_transit_days')),
        ])

        loader.conn.commit()
        print(f"reloaded {len(out)} rows into {ASSET_KEY}")