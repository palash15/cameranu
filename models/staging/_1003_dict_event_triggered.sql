with _0001 as (select * from {{ref('_0001_bigquery_primera')}})

select distinct
    user_pseudo_id,
    session_id,
    max(case when lower(event_name) = 'add_to_cart' then 1 else 0 end) as addtocart_triggered,
    max(case when lower(event_name) = 'purchase' then 1 else 0 end) as purchase_triggered,
    max(case when lower(event_name) = 'view_item' then 1 else 0 end) as viewitem_triggered,
    max(case when lower(event_name) = 'view_cart' then 1 else 0 end) as viewcart_triggered,
    max(case when lower(event_name) = 'begin_checkout' then 1 else 0 end) as checkout_triggered,
    max(case when lower(event_name) = 'view_item' then 1 else 0 end) as PDP_seen,
    max(case when lower(event_name) = 'view_item_list' then 1 else 0 end) as collection_page_seen,    
from _0001
group by user_pseudo_id, session_id