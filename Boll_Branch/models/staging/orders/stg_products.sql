-- Normalizing Producy table by unnesting the array
-- renamed _id to product_id and 
WITH source_products
AS (
	SELECT *,
		rank() OVER (
			PARTITION BY _id ORDER BY updated_at DESC
			) AS rn
	FROM {{ source('data-recruiting', 'products') }}
	),
unnested_array
AS (
	SELECT p._id AS product_id,
		p.title AS product_title,
		p.category AS product_category,
		p.created_at AS product_created_at,
		p.updated_at AS product_updated_at,
		v.*,
		p._loaded_at AS product_loaded_at
	FROM source_products p,
		unnest(variants) AS v
	WHERE rn = 1
	)
SELECT *
FROM unnested_array
