WITH _0001 AS (
    SELECT * FROM {{ ref("_1001_3_events_join") }} WHERE lower(event_name) != 'vwo'
),

_userinfo AS (
    SELECT
        user_pseudo_id,
        session_id,
        event_date,
        event_name,
        CASE 
            WHEN lower(event_name) IN ('payment_method', 'inwisselen_per_mail', 'inwisselen_nochoice', 
                                    'inwisselen_beslis_ik_later', 'inwisselen_per_post', 'filter_use', 
                                    'top_navigation_use', 'mobile_navigation_use', 'sort_use') 
            THEN 'client specific events'
            WHEN lower(event_name) LIKE '%cro%' -- This line is corrected
            THEN 'CRO events'
            WHEN lower(event_name) IN ('add_payment_info', 'add_shipping_info', 'add_to_cart', 'add_to_wishlist', 'begin_checkout', 
                                    'purchase', 'refund', 'remove_from_cart', 'select_item', 'select_promotion', 'view_cart', 
                                    'view_item', 'view_item_list', 'view_promotion') 
            THEN 'standard events'
            ELSE 'other events'
        END AS event_category,                            
        transaction_id,
        device,
        os_version AS os,
        browser,
        traffic_source,
        traffic_medium,
        revenue
    FROM _0001
)

SELECT * FROM _userinfo