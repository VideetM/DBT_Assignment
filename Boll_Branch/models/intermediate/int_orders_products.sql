WITH order_product_join
AS (
	SELECT o.order_id,
		o.subtotal,
		o.quantity,
		o.price * o.quantity AS total_line_revenue,
		o.line_total_discount,
		o.order_created_at,
		p.product_category,
		product_title,
		p.product_id,
		p.sku AS product_sku,
		p.option1 AS product_style,
		p.option2 AS product_size,
		row_number() OVER (
			PARTITION BY order_id ORDER BY order_id
			) AS dedupe_subtotal
	FROM {{ ref('stg_orders') }} o
	INNER JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id
		AND o.variant_id = p.variant_id
	),
final
AS (
	SELECT *
	FROM order_product_join
	)
SELECT *
FROM final
