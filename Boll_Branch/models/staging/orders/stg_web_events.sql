WITH source_events
AS (
	SELECT _id AS event_id,
		cookie_id,
		customer_id,
		event_name,
		event_url,
		event_properties,
		TIMESTAMP AS event_timestamp,
		utm_campaign,
		utm_source,
		utm_medium,
		_loaded_at AS event_loaded_at
	FROM {{ source('data-recruiting', 'web_events') }}
	)
SELECT *
FROM source_events
