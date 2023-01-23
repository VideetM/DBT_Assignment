WITH stg_orders
AS (
	SELECT *
	FROM {{ ref('stg_orders') }}
	),
stg_products
AS (
	SELECT *
	FROM {{ ref('stg_products') }}
	),
order_product_join --joining order and product staging table 
AS (
	SELECT orders.order_id,
		orders.subtotal,
		orders.quantity,
		orders.price * orders.quantity AS total_line_revenue,
		orders.line_total_discount,
		orders.order_created_at,
		products.product_category,
		products.product_title,
		products.product_id,
		products.product_sku,
		products.product_style,
		products.product_size,
		row_number() OVER (
			PARTITION BY order_id ORDER BY order_id
			) AS dedupe_subtotal --flag for deduping subtotal 
	FROM stg_orders orders
	INNER JOIN stg_products products ON orders.product_id = products.product_id
		AND orders.variant_id = products.variant_id
	),
final
AS (
	SELECT *
	FROM order_product_join
	)
SELECT *
FROM final
