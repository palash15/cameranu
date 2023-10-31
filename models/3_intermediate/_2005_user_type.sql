with _0001 as (select * from {{ref('_2001_6_events_join')}})

select distinct
    user_pseudo_id,
    event_date,
    session_id,
    session_count,
    case when session_count = 1 then 'new_user'
         when session_count > 1 then 'returning_user'
    end as user_type
from _0001
order by user_pseudo_id