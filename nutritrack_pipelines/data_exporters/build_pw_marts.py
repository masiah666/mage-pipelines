from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


MART_CALLS = """
DROP TABLE IF EXISTS port_mart.port_calls_by_type_month;
CREATE TABLE port_mart.port_calls_by_type_month AS
SELECT p.portid, p.portname, p.country, p.country_iso3, p.continent,
       c.calls_year, c.calls_month,
       make_date(c.calls_year::int, c.calls_month::int, 1) AS month_start,
       c.calls_container, c.calls_dry_bulk, c.calls_general_cargo,
       c.calls_roro, c.calls_tanker, c.calls_total
FROM port_raw.port_calls_monthly c
JOIN port_raw.portwatch_ports p USING (portid);
ALTER TABLE port_mart.port_calls_by_type_month
    ADD PRIMARY KEY (portid, calls_year, calls_month);
CREATE INDEX ON port_mart.port_calls_by_type_month (country_iso3);
"""

MART_CONNECTIONS = """
DROP TABLE IF EXISTS port_mart.port_connection_summary;
CREATE TABLE port_mart.port_connection_summary AS
SELECT c.from_portid, c.from_portname, c.from_country, c.from_iso3,
       c.to_portid, c.to_portname, c.to_country, c.to_iso3,
       t.continent AS to_continent,
       c.average_transit_days,
       c.daily_capacity_at_risk,
       c.relative_capacity_at_risk,
       (c.from_iso3 = c.to_iso3)                     AS is_domestic,
       ROW_NUMBER() OVER (PARTITION BY c.from_portid
                          ORDER BY c.daily_capacity_at_risk DESC NULLS LAST)
                                                     AS rank_from_port,
       ROW_NUMBER() OVER (PARTITION BY c.to_portid
                          ORDER BY c.daily_capacity_at_risk DESC NULLS LAST)
                                                     AS rank_to_port
FROM port_raw.port_connections c
LEFT JOIN port_raw.portwatch_ports t ON t.portid = c.to_portid;
ALTER TABLE port_mart.port_connection_summary
    ADD PRIMARY KEY (from_portid, to_portid);
CREATE INDEX ON port_mart.port_connection_summary (from_portid, rank_from_port);
CREATE INDEX ON port_mart.port_connection_summary (to_portid, rank_to_port);
"""

MARTS = [
    ('port_mart.port_calls_by_type_month', MART_CALLS,
     'Monthly calls per port by vessel type, joined to port reference',
     'one row per port per year per month',
     ['port_raw.port_calls_monthly', 'port_raw.portwatch_ports']),
    ('port_mart.port_connection_summary', MART_CONNECTIONS,
     'Directed port pairs ranked by capacity per origin and destination, with domestic flag',
     'one row per directed port pair',
     ['port_raw.port_connections', 'port_raw.portwatch_ports']),
]


@data_exporter
def export_data(df, **kwargs) -> None:
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    with Postgres.with_config(ConfigFileLoader(config_path, 'default')) as loader:
        for asset_key, sql, description, grain, upstreams in MARTS:
            loader.execute(sql)

            schema, table = asset_key.split('.')
            loader.execute(f"""
                INSERT INTO registry.assets (
                    asset_key, schema_name, table_name, layer, description,
                    source_system, source_detail, grain, owner,
                    pipeline_uuid, block_uuid, last_run_at, last_row_count
                ) VALUES (
                    '{asset_key}', '{schema}', '{table}', 'mart',
                    '{description}',
                    'Derived',
                    'Rebuilt from raw on every refresh',
                    '{grain}',
                    'Haisam',
                    '{kwargs.get("pipeline_uuid")}',
                    '{kwargs.get("block_uuid")}',
                    now(), (SELECT COUNT(*) FROM {asset_key})
                )
                ON CONFLICT (asset_key) DO UPDATE SET
                    last_run_at    = EXCLUDED.last_run_at,
                    last_row_count = EXCLUDED.last_row_count,
                    pipeline_uuid  = EXCLUDED.pipeline_uuid,
                    block_uuid     = EXCLUDED.block_uuid;
            """)

            for up in upstreams:
                loader.execute(f"""
                    INSERT INTO registry.edges (from_asset, to_asset, edge_type)
                    VALUES ('{up}', '{asset_key}', 'feeds')
                    ON CONFLICT DO NOTHING;
                """)

            loader.execute(f"""
                INSERT INTO registry.quality_checks
                    (asset_key, check_name, passed, detail, checked_at)
                SELECT '{asset_key}', 'has_rows',
                       COUNT(*) > 0, COUNT(*) || ' rows', now()
                FROM {asset_key}
                ON CONFLICT (asset_key, check_name) DO UPDATE SET
                    passed = EXCLUDED.passed, detail = EXCLUDED.detail,
                    checked_at = EXCLUDED.checked_at;
            """)

        loader.execute("""
            INSERT INTO registry.quality_checks
                (asset_key, check_name, passed, detail, checked_at)
            SELECT 'port_mart.port_calls_by_type_month', 'every_row_has_a_named_port',
                   COUNT(*) = (SELECT COUNT(*) FROM port_raw.port_calls_monthly),
                   COUNT(*) || ' of ' ||
                   (SELECT COUNT(*) FROM port_raw.port_calls_monthly) || ' raw rows joined',
                   now()
            FROM port_mart.port_calls_by_type_month
            ON CONFLICT (asset_key, check_name) DO UPDATE SET
                passed = EXCLUDED.passed, detail = EXCLUDED.detail,
                checked_at = EXCLUDED.checked_at;
        """)

        loader.conn.commit()
        print("both PortWatch marts rebuilt, registered, checked")