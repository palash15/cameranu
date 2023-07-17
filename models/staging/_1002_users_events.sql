with _0001 as (select * from {{ ref("_1001_events_transform") }} where lower(event_name)!='vwo')

select
    user_pseudo_id,
    session_id,
    event_date,
    event_name,
    case when lower(event_name) in ('payment_method', 'inwisselen_per_mail', 'inwisselen_nochoice', 
                                    'inwisselen_beslis_ik_later', 'inwisselen_per_post', 'filter_use', 
                                    'top_navigation_use', 'mobile_navigation_use', 'sort_use') 
        then 'client specific events'

        when starts_with(lower(event_name), 'cro') 
        then 'CRO events'

        when lower(event_name) in ('add_payment_info', 'add_shipping_info', 'add_to_cart', 'add_to_wishlist', 'begin_checkout', 
                                    'purchase', 'refund', 'remove_from_cart', 'select_item', 'select_promotion', 'view_cart', 
                                    'view_item', 'view_item_list', 'view_promotion') 
        then 'standard events'
        else 'other events'
    end as event_category,                            
    transaction_id,
    device,
    os_version as os,
    browser,
    traffic_source,
    traffic_medium,
    revenue
from _0001
    