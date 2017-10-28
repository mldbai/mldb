SELECT e.event_id,
    --  direct event infos
    e.name,
    --  e.keyword, e.importance,
    e.start_time,
    e.end_time,
    d.division, d.category,

    -- debug
    --  es.sku,
    --  es.price_cad,

    -- SKU AGGREGATES
    -- stats
    COUNT(es.sku) as nb_skus,
    MIN(es.price_cad), MAX(es.price_cad), AVG(es.price_cad),
    STDDEV_SAMP(es.price_cad) as price_cad_std,
    -- TODO MEDIAN
    -- price distribution
    sum(es.price_cad <= 10) as nb_lt_10,
    sum(es.price_cad > 10 and es.price_cad <= 20) as price_cad_nb_10_20,
    sum(es.price_cad > 20 and es.price_cad <= 35) as price_cad_nb_20_35,
    sum(es.price_cad > 35 and es.price_cad <= 50) as price_cad_nb_35_50,
    sum(es.price_cad > 50 and es.price_cad <= 100) as price_cad_nb_50_100,
    sum(es.price_cad > 100 and es.price_cad <= 500) as price_cad_nb_100_500,
    sum(es.price_cad > 500 and es.price_cad <= 1000) as price_cad_nb_500_1000,
    sum(es.price_cad > 1000) as price_cad_nb_1000_inf,
    -- all brands
    -- TODO also load the nb of sku for each brand
    GROUP_CONCAT(DISTINCT(es.brand) SEPARATOR '|') as brands
FROM events as e
    LEFT JOIN Event_Category_Div_rpt as d ON d.event_id = e.event_id
    LEFT JOIN event_skus as es ON es.event_id = e.event_id
WHERE
    e.start_time < now()
    AND e.start_time > '2012-01-01'
GROUP BY e.event_id
HAVING nb_skus > 0
--  ORDER BY e.event_id DESC
--  LIMIT 10000
