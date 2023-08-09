with _1000 as (
    select * from {{ref('_1000_users')}}
    where experiment_name is not null and event_category != 'other events'
),

dates as (
    select experiment_name as experiments, min(event_date) as min_date, max(event_date) as max_date 
    from _1000 
    group by experiment_name
),

metrics as (
    select
        experiment_name as experiments, 
        variant_name as variants,
        device,
        event_name
    from _1000
    group by experiment_name, variant_name, device, event_name
)

select
    dates.min_date,
    dates.max_date,
    metrics.*
from dates left join metrics on dates.experiments = metrics.experiments
