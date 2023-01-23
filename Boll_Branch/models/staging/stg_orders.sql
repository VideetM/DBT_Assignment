WITH source_orders
AS (
	SELECT *,
		rank() OVER (
			PARTITION BY _id ORDER BY updated_at DESC
			) AS rn
	-- ranking the records by updated at 
	FROM {{ source('data-recruiting', 'orders') }}
	),
unnested_array
AS (
	SELECT o._id AS order_id, --renaming columns appropriately 
		o.created_at AS order_created_at,
		o.updated_at AS order_updated_at,
		o.subtotal,
		o.total,
		l.*,
		o._loaded_at AS order_loaded_at
	FROM source_orders o,
		unnest(line_items) AS l -- unnesting array for latest updated line_item 
	WHERE rn = 1 --picking the latest updated record for line items
	),
final
AS (
	SELECT *
	FROM unnested_array
	)
SELECT *
FROM final
