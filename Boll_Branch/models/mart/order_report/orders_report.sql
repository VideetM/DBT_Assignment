WITH final
AS (
	SELECT *
	FROM {{ ref('int_orders_products') }}
	)
SELECT *
FROM final
