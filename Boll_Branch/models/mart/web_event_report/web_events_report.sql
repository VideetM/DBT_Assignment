WITH final
AS (
	SELECT cookie_id,
		customer_id,
		web_user_id,
		event_name,
		event_url,
		event_timestamp,
		utm_campaign,
		utm_medium,
		utm_source,
		global_session_id,
		user_session_id,
		order_id,
		product_id,
		landing_page_url,
		landing_page_timestamp,
		bounced_flag,
	FROM {{ ref('int_web_events') }}
	)
SELECT *
FROM final
