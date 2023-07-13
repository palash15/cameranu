with _0001 as (select * from {{ ref("_0001_bigquery_primera") }})

select
    user_pseudo_id,
    session_id,
    event_date,
    event_name,
    transaction_id,
    device,
    os_version as os,
    browser,
    traffic_source,
    traffic_medium,
    revenue
from _0001
    