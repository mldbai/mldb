SELECT
    oh.uid, oh.order_date, ol.event_id, ol.sku
    -- debug
    --  oh.order_status, ol.line_type, ol.status
    --  oh.order_id, ol.value_cad, oh.total_order_value_cad
FROM order_lines as ol
    JOIN order_headers as oh ON ol.order_id = oh.order_id
    JOIN users_info as ui ON (ui.uid = oh.uid)
WHERE
    -- canada and recent
    ui.country = 'CA'
    AND oh.order_date > '2012-01-01'
    -- this is what makes it a purchase for our purposes
    AND oh.order_status = 'SHIPPED'
    AND ol.line_type = 'SKU'
    AND ol.status = 40
--  ORDER BY oh.order_id DESC
--  LIMIT 10000
