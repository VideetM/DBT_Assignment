WITH final
AS (
	SELECT *
	FROM {{ ref('int_web_events') }}
	)
SELECT *
FROM final
