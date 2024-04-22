with _3000_filter as (
    select 
        *, 
        min(event_date) over (partition by experiment_name) as min_date,
        dense_rank() over (partition by experiment_name order by event_date desc) as num_days
    from {{ref('_3000_users')}}
)
-- ,

-- _3000_filter as (
--     select
--         *,
--         case when date_diff(event_date, min_date, day) > 30 then 'exclude'
--             else 'include'
--         end as filter_date
--     from _3000    
-- )

select *
from _3000_filter