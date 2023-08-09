with _1000 as (
    select * from {{ref('_1000_users')}}
    WHERE EVENT_CATEGORY != 'other events' AND EXPERIMENT_NAME IS NOT NULL
),

-- This code was written to optimize the latency will loading data in a dashboard. It loads 15 times less data.

groupby as (
  select 
    event_date, 
    event_name,
    experiment_name,
    variant_name,
    transaction_id,
    revenue,
    device,
    browser,
    user_type,
    channel_grouping_session
    count(distinct user_pseudo_id) as users
  from big-query-project-primera.dbt_production._9001_device_data
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

SELECT * from groupby

  
  