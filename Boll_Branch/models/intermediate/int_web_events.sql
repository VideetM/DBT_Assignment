WITH int_web_events
AS (
	SELECT *
	FROM {{ ref('stg_web_events') }}
	),
web_session_logic --cte houses logic for counting user's unique session and global unique session
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
				) AS global_session_id, --used for total web session 
			SUM(is_new_session) OVER (
				PARTITION BY cookie_id ORDER BY event_timestamp
				) AS user_session_id,
			order_id,
			cast(product_id as int) product_id
		FROM (
			SELECT *,
				CASE 
					WHEN TIMESTAMP_DIFF(event_timestamp, last_event, MINUTE) >= 30
						OR last_event IS NULL
						THEN 1
					ELSE 0
					END AS is_new_session --used for user's total web session
			FROM (
				SELECT *,
					LAG(event_timestamp, 1) OVER (
						PARTITION BY cookie_id ORDER BY event_timestamp
						) AS last_event
				FROM int_web_events
				)
			ORDER BY cookie_id DESC
			)
		)
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
			) AS landing_page_flag --flag for 1st landing page url for a  web session
	FROM web_session_logic
	),
first_landing_page_timestamp --logic for 1st landing page timestamp for a web session
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
bounced_session --logic for bounced session
AS (
	SELECT global_session_id,
		CASE 
			WHEN count(event_url) <= 1
				THEN 1
			ELSE 0
			END AS bounced_flag --flag for bounced web session
	FROM first_landing_page_timestamp
	WHERE event_name = 'page'
	GROUP BY global_session_id
	),
final
AS (
	SELECT fl.*,
		b.bounced_flag
	FROM first_landing_page_timestamp fl
	LEFT JOIN bounced_session b ON fl.global_session_id = b.global_session_id
	)
SELECT *
FROM final
