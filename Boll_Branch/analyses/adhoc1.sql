-- 1st adhoc query

WITH order_count_sheet
AS (
	SELECT count(DISTINCT order_id) AS sheet_count
	FROM `ae_data_challenge_v1_vmalpe.order_report`
	WHERE product_category = 'Sheet Sets'
		AND product_style LIKE '%King%'
	),
total_order
AS (
	SELECT count(DISTINCT order_id) AS order_count
	FROM `ae_data_challenge_v1_vmalpe.order_report`
	)
SELECT round((sheet_count / order_count), 3) * 100 AS sheet_order_percentage
FROM total_order,
	order_count_sheet

-- 0.4% proportion of orders contained a product with category “Sheet Sets” and size “King”

-- 2nd adhoc query

SELECT product_sku,
	sum(total_line_revenue) - sum(coalesce(line_total_discount, 0)) AS gross_revenue
FROM `ae_data_challenge_v1_vmalpe.order_report`
GROUP BY product_sku
ORDER BY 2 DESC limit 1

-- product_sku -DSETKIS30HWT with highest revenue

-- 3rd adhoc query

SELECT *
FROM `data - recruiting.ae_data_challenge_v1_vmalpe.order_count`
ORDER BY avg_order_unit DESC

--2022-08-03 with 1.374 avg unit per order. calculated using metrics called avg_order_unit. details in schema.yml.

-- 4th adhoc query
WITH plush_order_session
AS (
	SELECT count(DISTINCT global_session_id) AS plush_order_session
	FROM `data - recruiting.ae_data_challenge_v1_vmalpe.web_events_report` w
	INNER JOIN `data - recruiting.ae_data_challenge_v1_vmalpe.order_report` o ON w.order_id = o.order_id
	WHERE o.product_title = 'Plush Bath Towel Set'
		AND w.event_name = 'order_completed'
	),
total_plush_session
AS (
	SELECT count(DISTINCT global_session_id) AS total_plush_session
	FROM `data - recruiting.ae_data_challenge_v1_vmalpe.web_events_report` a
	INNER JOIN `data - recruiting.ae_data_challenge_v1_vmalpe.order_report` b ON cast(a.product_id AS INT) = b.product_id
	WHERE b.product_title = 'Plush Bath Towel Set'
	)
SELECT round((plush_order_session / total_plush_session), 2) * 100 AS plush_conversion_rate
FROM plush_order_session,
	total_plush_session


-- 9% conversion rate, rounded to 3 decimals.

--5th adhoc query

WITH next_events
AS (
	SELECT *,
		LEAD(event_url) OVER (
			PARTITION BY cookie_id,
			user_session_id ORDER BY event_timestamp
			) AS next_event
	FROM `data - recruiting.ae_data_challenge_v1_vmalpe.web_events_report`
	WHERE event_name = 'page'
	ORDER BY user_session_id,
		event_timestamp
	)
SELECT count(CASE 
			WHEN next_event LIKE '%checkout.bollandbranch.com%'
				THEN global_session_id
			END) AS total_checkout,
	event_url
FROM next_events
GROUP BY 2
ORDER BY 1 DESC limit 5


-- 1 -https://www.bollandbranch.com/products/signature-hemmed-sheet-set?color=Spruce%20-%20Limited%20Edition
-- 2 -https://www.bollandbranch.com/
-- 3 -https://checkout.bollandbranch.com/account/login?return_url=%2Faccount
-- 4 -https://www.bollandbranch.com/?utm_source=other&utm_medium=QA
-- 5 -https://www.bollandbranch.com/products/signature-hemmed-sheet-set?color=White

--6th adhoc query

SELECT utm_campaign,
	count(DISTINCT web_user_id) AS total_web_user
FROM `data - recruiting.ae_data_challenge_v1_vmalpe.web_events_report`
WHERE utm_campaign IS NOT NULL
GROUP BY utm_campaign
ORDER BY 2 DESC limit 5

-- 1- g|search|brand|core|desktop|exact
-- 2 -Boll&Branch_Prospecting_April2022_Broad+Lookalike_Influencer
-- 3 -73861
-- 4 -instagram_daniaustin
-- 5 -g|search|brand|plus|desktop|exact

--7th adhoc query

SELECT w.utm_source,
	sum(total_line_revenue) - sum(line_total_discount) AS gross_revenue
FROM `data - recruiting.ae_data_challenge_v1_vmalpe.web_events_report` w
INNER JOIN `data - recruiting.ae_data_challenge_v1_vmalpe.order_report` o ON w.order_id = o.order_id
WHERE w.utm_source IS NOT NULL
GROUP BY w.utm_source
ORDER BY 2 DESC limit 5

--no non null sources that garnered revenue exist.

