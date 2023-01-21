WITH int_web_events
AS (
	SELECT *
	FROM {{ ref('stg_web_events') }}
	)
SELECT *
FROM int_web_events
