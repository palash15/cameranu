with _1000 as (select * from {{ref('_2000_users')}})

select * from _1000
