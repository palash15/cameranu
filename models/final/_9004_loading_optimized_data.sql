with _1000 as (
    select * from {{ref('_1000_users')}}
),

-- This code was written to optimize the latency will loading data in a dashboard. It loads 15 times less data.

groupby as (
  select 
    event_date, 
    event_name,
    event_category,
    transaction_id,
    device,
    browser,
    traffic_source,
    traffic_medium,
    revenue,
    experiment_name,
    variant_name,
    add_to_cart_triggered as addtocart_triggered,
    purchase_triggered,
    view_item_triggered as viewitem_triggered,
    view_cart_triggered as viewcart_triggered,
    begin_checkout_triggered as checkout_triggered,
    pdp_seen,
    collection_page_seen,
    user_type,
    channel_grouping_session,
    count(distinct user_pseudo_id) as users, 
    count(distinct session_id) as sessions,
  from big-query-project-primera.dbt_production._9001_device_data
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)

SELECT distinct event_date, event_name, experiment_name, variant_name, users, transaction_id, revenue, device, browser, user_type
from groupby
WHERE EVENT_CATEGORY != 'other events' AND EXPERIMENT_NAME IS NOT NULL
  
  