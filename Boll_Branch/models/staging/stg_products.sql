WITH source_products
AS (
	SELECT *,
		rank() OVER (
			PARTITION BY _id ORDER BY updated_at DESC
			) AS rn --  ranking the records by updated at
	FROM {{ source('data-recruiting', 'products') }}
	),
unnested_array
AS (
	SELECT p._id AS product_id, --renaming columns appropriately 
		p.title AS product_title,
		p.category AS product_category,
		p.created_at AS product_created_at,
		p.updated_at AS product_updated_at,
		v.*,
		p._loaded_at AS product_loaded_at
	FROM source_products p,
		unnest(variants) AS v -- unnesting array for latest updated varaints 
	WHERE rn = 1 --picking the latest updated record for variants  
	),
base_product
AS ( select 
	* FROM unnested_array
	),
final
AS (
	SELECT product_id,
		product_title,
		product_category,
		sku as product_sku,
		variant_id,
		option1 AS product_style, --renaming based on values i observed generally
		option2 AS product_size, --renaming based on values i observed generally
		product_created_at,
		product_updated_at,
		product_loaded_at
	FROM base_product
	)
SELECT *
FROM final
