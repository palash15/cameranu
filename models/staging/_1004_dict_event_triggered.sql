with _0001 as (select * from {{ref('_1001_events_transform')}})

select distinct
    user_pseudo_id,
    session_id,
    max(case when lower(event_name) = 'add_to_cart' then 'add_to_cart' else null end) as addtocart_triggered,
    max(case when lower(event_name) = 'purchase' then 'purchase' else null end) as purchase_triggered,
    max(case when lower(event_name) = 'view_item' then 'view_item' else null end) as viewitem_triggered,
    max(case when lower(event_name) = 'view_cart' then 'view_cart' else null end) as viewcart_triggered,
    max(case when lower(event_name) = 'begin_checkout' then 'begin_checkout' else null end) as checkout_triggered,
    max(case when lower(event_name) = 'view_item' then 'PDP' else null end) as PDP_seen,
    max(case when lower(event_name) = 'view_item_list' then 'PLP' else null end) as collection_page_seen,    
from _0001
group by user_pseudo_id, session_id