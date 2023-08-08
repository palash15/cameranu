with _1000 as (
    select * from {{ref('_1000_users')}}
    where experiment_name is not null and event_category != 'other events'
)

select
    min(event_date) as min_date,
    max(event_date) as max_date,
    experiment_name as experiments, 
    variant_name as variants,
    device,
    browser,
    event_name,
    count(distinct transaction_id) as purchases,
    sum(revenue) as revenue,
    count(distinct user_pseudo_id) as users,
    count(distinct session_id) as sessions,
    count(case when user_type='new_user' then 1 else 0 end) as new_users,
    count(case when user_type='returning_user' then 1 else 0 end) as returning_users

from _1000
group by experiment_name, variant_name, device, browser, event_name
