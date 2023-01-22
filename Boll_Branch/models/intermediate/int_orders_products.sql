WITH order_product_join
AS (
	SELECT o.order_id,
		o.price * o.quantity AS total_line_revenue,
		o.line_total_discount,
		o.order_created_at,
		p.product_category,
		product_title,
		p.sku AS product_sku,
		p.option1 AS product_style,
		p.option2 AS product_size,
		sum(price * quantity) OVER (PARTITION BY order_id) AS line_item_price,
		sum(line_total_discount) OVER (PARTITION BY order_id) AS line_item_discount,
		sum(quantity) OVER (PARTITION BY order_id) AS total_order_unit,
		avg(subtotal) OVER (PARTITION BY order_id) AS avg_order_value
	FROM {{ ref('stg_orders') }} o
	INNER JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id
		AND o.variant_id = p.variant_id
	),
final
AS (
	SELECT *,
		avg(total_order_unit) OVER (PARTITION BY order_id) AS avg_order_unit,
		line_item_price - coalesce(line_item_discount, 0) AS total_gross_revenue
	FROM order_product_join
	)
SELECT *
FROM final
