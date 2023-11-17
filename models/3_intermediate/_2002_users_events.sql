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
            'footer_social','search','scroll_to_top','formSend','user_menu','compare_products','kitsamenstelling_openen',
            'singlePush','video_start','video_complete','footer_menu','spec_search','scroll','form_submit','refund','sign_up',
            'form_start','chat_widget','leadgen_send','first_visit','click','combodeal_checkbox','button_click',
            'session_start','combodeal_alternative','add_to_cart_slidein','view_search_results','file_download','quick_menu',
            'header_click','footer_contact','video_progress','user_engagement','filters','homepage_events','click_handig_links',
            'combodeal_alternative_slidein','kitsamenstelling_product_klik','begin_checkout','top_menu','floating_menu'
            ) THEN 'client specific events'
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