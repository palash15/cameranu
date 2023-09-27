with _1000 as (
    select * from {{ref('_1000_users')}}
    WHERE EVENT_CATEGORY != 'other events' AND EXPERIMENT_NAME IS NOT NULL
)

select
    experiment_name as experiments, 
    min(event_date) as min_date, 
    max(event_date) as max_date,
    string_agg(distinct variant_name, ',') as variants,
    string_agg(distinct device, ',') as devices,
    string_agg(distinct event_name, ',') as event_names,
    'Primera' as project
from _1000
group by experiment_name
