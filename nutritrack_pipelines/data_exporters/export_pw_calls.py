from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from nutritrack_pipelines.utils.quality import (
    run_checks, has_rows, no_duplicate_keys, no_nulls, non_negative,
)

ASSET_KEY = 'port_raw.port_calls_monthly'

RENAME = {
    'portid': 'portid',
    'year': 'calls_year',
    'month': 'calls_month',
    'container': 'calls_container',
    'dry_bulk': 'calls_dry_bulk',
    'general_cargo': 'calls_general_cargo',
    'roro': 'calls_roro',
    'tanker': 'calls_tanker',
    'total': 'calls_total',
}

BATCH = 1000


def sql_num(v):
    if v is None or v != v:
        return 'NULL'
    return str(int(v))


@data_exporter
def export_data(df: DataFrame, **kwargs) -> None:
    out = df[list(RENAME)].rename(columns=RENAME)

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:

        cols = list(out.columns)
        col_list = ', '.join(cols)
        updates = ', '.join(
            f"{c} = EXCLUDED.{c}"
            for c in cols if c not in ('portid', 'calls_year', 'calls_month')
        )
        for start in range(0, len(out), BATCH):
            chunk = out.iloc[start:start + BATCH]
            values = ',\n'.join(
                '(' + "'" + str(row[0]).replace("'", "''") + "', "
                + ', '.join(sql_num(v) for v in row[1:]) + ')'
                for row in chunk.itertuples(index=False)
            )
            loader.execute(f"""
                INSERT INTO port_raw.port_calls_monthly ({col_list})
                VALUES {values}
                ON CONFLICT (portid, calls_year, calls_month)
                DO UPDATE SET {updates};
            """)

        loader.execute(f"""
            INSERT INTO registry.assets (
                asset_key, schema_name, table_name, layer, description,
                source_system, source_detail, grain, owner,
                pipeline_uuid, block_uuid, last_run_at, last_row_count
            ) VALUES (
                '{ASSET_KEY}', 'port_raw', 'port_calls_monthly', 'raw',
                'Monthly vessel calls per port by vessel type, rolled up server-side from daily data',
                'IMF PortWatch',
                'Daily_Ports_Data',
                'one row per port per year per month',
                'Haisam',
                '{kwargs.get("pipeline_uuid")}',
                '{kwargs.get("block_uuid")}',
                now(), (SELECT COUNT(*) FROM port_raw.port_calls_monthly)
            )
            ON CONFLICT (asset_key) DO UPDATE SET
                last_run_at    = EXCLUDED.last_run_at,
                last_row_count = EXCLUDED.last_row_count,
                pipeline_uuid  = EXCLUDED.pipeline_uuid,
                block_uuid     = EXCLUDED.block_uuid;
        """)

        run_checks(loader, ASSET_KEY, out, [
            ('has_rows', has_rows),
            ('unique_port_month', no_duplicate_keys('portid', 'calls_year', 'calls_month')),
            ('key_fields_present', no_nulls('portid', 'calls_year', 'calls_month')),
            ('calls_non_negative', non_negative('calls_total')),
        ])

        loader.conn.commit()
        print(f"upserted {len(out)} rows; table registered")