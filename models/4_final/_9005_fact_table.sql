with _1000 as (
    select * from {{ref('_3000_users')}}
    WHERE EVENT_CATEGORY != 'other events' AND EXPERIMENT_NAME IS NOT NULL
),

agg as (
    select
        experiment_name as experiments, 
        min(event_date) as min_date, 
        max(event_date) as max_date,
        string_agg(distinct variant_name, ',') as variants,
        string_agg(distinct device, ',') as devices,
        string_agg(distinct event_name, ',') as event_names,
        'Cameranu' as user_role,
        'Cameranu' as project
    from _1000
    group by experiment_name
)

select
    experiments,
    min_date,
    case when date_diff(max_date, min_date, day) > 30 then date_add(min_date, INTERVAL 30 DAY)
         else max_date
    end as max_date,
    variants,
    devices,
    event_names,
    user_role,
    project
from agg