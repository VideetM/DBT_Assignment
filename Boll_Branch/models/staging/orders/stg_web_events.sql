WITH source_events
AS (
	SELECT _id as event_id,
    cookie_id,
    customer_id,
    event_name,
    event_url,
    event_properties,
    timestamp as event_timestamp,
    utm_campaign,
    utm_source,
    utm_medium,
    _loaded_at as event_loaded_at


	FROM {{ source('data-recruiting', 'web_events') }}
	)


    select * 
    from source_events