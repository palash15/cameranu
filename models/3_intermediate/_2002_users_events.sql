WITH _0001 AS (
    SELECT * FROM {{ ref("_2001_6_events_join") }} WHERE lower(event_name) != 'vwo'
),

_userinfo AS (
    SELECT
        user_pseudo_id,
        session_id,
        event_date,
        event_name,
        CASE 
            WHEN lower(event_name) IN (
                'payment_method', 'inwisselen_per_mail', 'inwisselen_nochoice', 
                'inwisselen_beslis_ik_later', 'inwisselen_per_post', 'filter_use', 
                'top_navigation_use', 'mobile_navigation_use', 'sort_use',
                'cart_dropdown', 'bekijk_winkelvoorraad_button_clicked', 'tooltip_open',
                'bekijk_winkelvoorraad_store_click', 'inwisselen_gelegenheden',
                'tab_usage', 'bekijk_winkelvoorraad_search', 'pbl_search',
                'stock_check', 'inwisselen_per_post', 'pbl_pageview', 'pbl_link_click',
                'account_login', 'quick_link_use', 'promo_banner_use',
                'phone_number_clicked', 'file_download', 'zoom_image', 'account_register',
                'bekijk_winkelvoorraad_button_available', 'mega_menu_use',
                'inwisselen_nochoice', 'coupon_code_added', 'pbl_file_download',
                'top_navigation_use', 'inwisselen_per_mail', 'inwisselen_beslis_ik_later',
                'pbl_user_navigation', 'bekijk_winkelvoorraad_contact_click'
            ) THEN 'client specific events'
            WHEN lower(event_name) LIKE 'cro%'
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