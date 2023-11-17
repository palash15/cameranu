{{config(materialized='table')}}
with _1000 as (select * from {{ref('_3000_users')}})

select
    user_pseudo_id,
    session_id,
    event_date,
    event_name,
    transaction_id,
    device,
    os,
    browser,
    traffic_source,
    traffic_medium,
    revenue,
    experiment_id,
    experiment_name,
    variant_id,
    variant_name,
    channel_grouping_session    
from _1000