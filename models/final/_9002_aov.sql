with _1000 as (select * from {{ref('_1000_users')}} where experiment_name is not null)

select
    event_date,
    vwo_uuid,
    experiment_id,
    experiment_name,
    variant_id,
    variant_name,
    transaction_id,
    revenue,
    addtocart_triggered,
    purchase_triggered,
    viewitem_triggered,
    viewcart_triggered,
    checkout_triggered,
    pdp_seen,
    collection_page_seen,    
from _1000