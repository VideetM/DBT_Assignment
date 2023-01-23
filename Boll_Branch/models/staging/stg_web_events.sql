WITH source_events
AS (
	SELECT _id AS event_id, --renaming columns appropriately 
		cookie_id,
		customer_id,
		event_name,
		event_url,
		event_properties,
		CASE 
			WHEN json_value(event_properties, '$.order_id') IS NOT NULL
				THEN json_value(event_properties, '$.order_id')
			END AS order_id, -- extracting order id from event_properties struct
		CASE 
			WHEN json_value(event_properties, '$.product_id') IS NOT NULL
				THEN json_value(event_properties, '$.product_id')
			END AS product_id, -- extracting product id from event_properties struct
		coalesce(customer_id, cookie_id) AS web_user_id, --web user id for counting unique web users
		TIMESTAMP AS event_timestamp,
		utm_campaign,
		utm_source,
		utm_medium,
		_loaded_at AS event_loaded_at
	FROM {{ source('data-recruiting', 'web_events') }}
	),
final
AS (
	SELECT *
	FROM source_events
	)
SELECT *
FROM final
