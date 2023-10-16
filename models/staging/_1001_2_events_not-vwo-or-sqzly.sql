with events as (select * from {{ref("_0001_bigquery_primera")}}),

clean_events as (
  SELECT 
    PARSE_DATE("%Y%m%d", event_date) as event_date,
    user_pseudo_id,
    (select value.int_value from unnest(event_params) where key = "ga_session_id") as session_id,
    geo.country as country,  
    event_name,
    (select value.string_value from unnest(event_params) where key = "transaction_id") as transaction_id,
    (select value.string_value from unnest(event_params) where key = "page_location") as page_location,
    traffic_source.source as traffic_source,
    traffic_source.medium as traffic_medium,
    event_bundle_sequence_id,
    device.category as device,
    device.mobile_brand_name as mobile_brand,
    device.mobile_model_name as mobile_model,
    device.web_info.browser as browser,
    device.operating_system_version as os_version,
    device.is_limited_ad_tracking as limited_ad_tracking,
    (select value.int_value from unnest(event_params) where key = "ga_session_number") as session_count,
    ecommerce.purchase_revenue as revenue,
    ecommerce.refund_value as refund_value,
    ecommerce.total_item_quantity as item_quantity,
  FROM events
)

select * from clean_events where session_id is not null