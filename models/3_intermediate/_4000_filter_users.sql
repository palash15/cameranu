with _3000 as (
    select *, min(event_date) over (partition by experiment_name) as min_date
    from {{ref('_3000_users')}}
),

_3000_filter as (
    select
        *,
        case when date_diff(event_date, min_date, day) > 30 then 'exclude'
            else 'include'
        end as filter_date
    from _3000    
)

select *
from _3000_filter
where filter_date = 'include' 