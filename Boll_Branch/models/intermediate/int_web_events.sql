WITH int_web_events
AS (
	SELECT *
	FROM {{ ref('stg_web_events') }}
	),
extractiom_event_properties
AS (
	SELECT *,
		CASE 
			WHEN json_value(event_properties, '$.order_id') IS NOT NULL
				THEN json_value(event_properties, '$.order_id')
			END AS order_id,
		CASE 
			WHEN json_value(event_properties, '$.product_id') IS NOT NULL
				THEN json_value(event_properties, '$.product_id')
			END AS product_id,
		coalesce(customer_id, cookie_id) AS web_user_id
	FROM int_web_events
	),
session_logic
AS (
	SELECT *
	FROM (
		SELECT cookie_id,
			customer_id,
			web_user_id,
			event_name,
			event_url,
			event_properties,
			event_timestamp,
			last_event,
			TIMESTAMP_DIFF(event_timestamp, last_event, MINUTE) AS b,
			utm_campaign,
			utm_medium,
			utm_source,
			SUM(is_new_session) OVER (
				ORDER BY cookie_id,
					event_timestamp
				) AS global_session_id,
			SUM(is_new_session) OVER (
				PARTITION BY cookie_id ORDER BY event_timestamp
				) AS user_session_id,
			order_id,
			product_id
		FROM (
			SELECT *,
				CASE 
					WHEN TIMESTAMP_DIFF(event_timestamp, last_event, MINUTE) >= 30
						OR last_event IS NULL
						THEN 1
					ELSE 0
					END AS is_new_session
			FROM (
				SELECT *,
					LAG(event_timestamp, 1) OVER (
						PARTITION BY cookie_id ORDER BY event_timestamp
						) AS last_event
				FROM extractiom_event_properties
				)
			ORDER BY cookie_id DESC
			)
		)
	-- where user_session_id=2
	-- limit 1000
	--WHERE cookie_id = '0016c2d7-95b2-49d4-8ec3-53fa19921519'
	ORDER BY user_session_id
	),
base_session
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
		row_number() OVER (
			PARTITION BY cookie_id,
			user_session_id ORDER BY event_timestamp
			) AS landing_page_flag,
	-- first_value(event_url) OVER (
	-- 	PARTITION BY cookie_id,
	-- 	user_session_id ORDER BY event_timestamp
	-- 	) AS landing_page_url,
	-- min(event_timestamp) OVER (
	-- 	PARTITION BY cookie_id,
	-- 	user_session_id ORDER BY event_timestamp
	-- 	) AS landing_page_timestamp
	FROM session_logic
	),
first_landing_page_timestamp
AS (
	SELECT bs.*,
		lf.event_url AS landing_page_url,
		lf.event_timestamp AS landing_page_timestamp
	FROM base_session bs
	INNER JOIN (
		SELECT *
		FROM base_session
		WHERE landing_page_flag = 1
		) lf ON bs.cookie_id = lf.cookie_id
		AND bs.user_session_id = lf.user_session_id
	),
bounced_session
AS (
	SELECT global_session_id,
		CASE 
			WHEN count(event_url) <= 1
				THEN 1
			ELSE 0
			END AS bounced_flag
	FROM first_landing_page_timestamp
	WHERE event_name = 'page'
	GROUP BY global_session_id
	),
final
AS (
	SELECT a.*,
		b.bounced_flag
	FROM first_landing_page_timestamp a
	LEFT JOIN bounced_session b ON a.global_session_id = b.global_session_id
	)
SELECT *
FROM final
