WITH final
AS (
	SELECT o.*, w.utm_campaign,w.utm_medium,w.utm_source
	FROM {{ ref('int_orders_products') }} o
	left join {{ ref('int_web_events') }} w on o.order_id=w.order_id
	)
SELECT *
FROM final
