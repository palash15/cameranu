with _1000 as (
    select * from {{ref('_1000_users')}}
),

-- This code was written to optimize the latency will loading data in a dashboard. It loads 15 times less data.

groupby as (
  select 
    event_date, 
    event_name,
    transaction_id,
    device,
    browser,
    traffic_source,
    traffic_medium,
    revenue,
    experiment_name,
    variant_name,
    addtocart_triggered,
    purchase_triggered,
    viewitem_triggered,
    viewcart_triggered,
    checkout_triggered,
    pdp_seen,
    collection_page_seen,
    user_type,
    channel_grouping_session,
    count(distinct user_pseudo_id) as users, 
    count(distinct session_id) as sessions,
  from big-query-project-primera.dbt_production._9001_device_data
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)

  select * from groupby