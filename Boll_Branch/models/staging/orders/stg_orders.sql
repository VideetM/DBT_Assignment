-- Normalizing Order table by unnesting the array
-- renamed _id to order_id

WITH source_orders
AS (
	SELECT o._id AS order_id,o.created_at as order_created_at,o.updated_at as order_updated_at,
		o.subtotal,
		o.total,
		l.*,
        o._loaded_at as order_loaded_at
	FROM {{ source('data-recruiting', 'orders') }} o,
		unnest(line_items) AS l
	)
SELECT *
FROM source_orders
