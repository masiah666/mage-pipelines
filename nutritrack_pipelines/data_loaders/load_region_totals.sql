-- Docs: https://docs.mage.ai/guides/sql-blocks
SELECT
    d.region_code,
    d.region_name,
    t.traffic_year,
    SUM(t.teu)::bigint       AS total_teu,
    COUNT(*)                 AS countries_reporting,
    ROUND(AVG(t.teu))::bigint AS mean_teu
FROM port_raw.country_port_traffic t
JOIN port_raw.country_dim d
  ON d.country_iso3 = t.country_iso3
WHERE d.is_aggregate = false
GROUP BY d.region_code, d.region_name, t.traffic_year
ORDER BY d.region_name, t.traffic_year;