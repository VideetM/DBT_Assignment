WITH final
AS (
	SELECT event_id,
		cookie_id,
		customer_id,
		event_name,
		event_url,
		event_properties,
		event_timestamp,
		utm_campaign,
		utm_source,
		utm_medium
	FROM {{ ref('int_web_events') }}
	)
SELECT *
FROM final
