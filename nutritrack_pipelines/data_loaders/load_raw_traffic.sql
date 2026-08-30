-- Docs: https://docs.mage.ai/guides/sql-blocks
SELECT
    t.country_iso3,
    t.country_name,
    t.traffic_year,
    t.teu,
    d.region_code,
    d.region_name,
    d.longitude,
    d.latitude
FROM port_raw.country_port_traffic t
JOIN port_raw.country_dim d
  ON d.country_iso3 = t.country_iso3
WHERE d.is_aggregate = false;