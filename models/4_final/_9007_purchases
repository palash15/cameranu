with events as (select * from {{ref("_1001_bigquery_cameranu")}} where event_name='purchase'),

clean_events as (
    SELECT
        PARSE_DATE("%Y%m%d", event_date) as event_date,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
        geo.country as country,  
        device.category as device,
        device.mobile_brand_name as mobile_brand,
        device.mobile_model_name as mobile_model,
        device.web_info.browser as browser,
        device.operating_system_version as os_version,
        i.item_name as item_name,
        i.item_variant as item_variant,
        i.item_brand as item_brand,
        i.price as item_price,
        i.quantity as item_quantity,
        i.item_revenue as item_revenue,
    FROM events, unnest(items) i
)

select * from clean_events