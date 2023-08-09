with _1000 as (
    select * from {{ref('_1000_users')}}
    where experiment_name is not null and event_category != 'other events'
)

select
    experiment_name as experiments, 
    min(event_date) as min_date, 
    max(event_date) as max_date,
    string_agg(distinct variant_name, ',') as variants,
    string_agg(distinct device, ',') as devices,
    string_agg(distinct event_name, ',') as event_names
from _1000
group by experiment_name
