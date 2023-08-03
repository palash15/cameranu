with
    _1002 as (select * from {{ ref("_1002_users_events") }}),
    _1003 as (select * from {{ ref("_1003_test_library") }}),
    _1004 as (select * from {{ ref("_1004_dict_event_triggered") }}),
    _1005 as (select * from {{ref("_1005_user_type")}}),
    _1006 as (select * from {{ ref("_1006_dict_source_medium_conversion") }})

select
    t1.user_pseudo_id,
    t1.session_id,
    t1.event_date,
    t1.event_name,
    t1.event_category,
    t1.transaction_id,
    t1.device,
    t1.os,
    t1.browser,
    t1.traffic_source,
    t1.traffic_medium,
    t1.revenue,

    t2.vwo_uuid,
    t2.experiment_id,
    t2.experiment_name,
    t2.variant_id,
    t2.variant_name,
    t2.variant_type,

    t3.payment_method_triggered,
    t3.inwisselen_per_mail_triggered,
    t3.inwisselen_nochoice_triggered,
    t3.inwisselen_beslis_ik_later_triggered,
    t3.inwisselen_per_post_triggered,
    t3.filter_use_triggered,
    t3.top_navigation_use_triggered,
    t3.mobile_navigation_use_triggered,
    t3.sort_use_triggered,
    t3.add_payment_info_triggered,
    t3.add_shipping_info_triggered,
    t3.add_to_cart_triggered,
    t3.add_to_wishlist_triggered,
    t3.begin_checkout_triggered,
    t3.purchase_triggered,
    t3.refund_triggered,
    t3.remove_from_cart_triggered,
    t3.select_item_triggered,
    t3.select_promotion_triggered,
    t3.view_cart_triggered,
    t3.view_item_triggered,
    t3.view_item_list_triggered,
    t3.view_promotion_triggered,
    t3.cro_event_1_triggered,
    t3.cro_event_2_triggered,
    t3.cro_event_3_triggered,
    t3.cro_event_4_triggered,
    t3.cro_event_5_triggered,
    t3.cro_event_6_triggered,
    t3.cro_event_7_triggered,
    t3.cro_event_8_triggered,
    t3.cro_event_9_triggered,
    t3.cro_event_10_triggered,
    t3.cro_event_11_triggered,
    t3.cro_event_12_triggered,
    t3.cro_event_13_triggered,
    t3.cro_event_14_triggered,
    t3.cro_event_15_triggered,
    t3.cro_event_16_triggered,
    t3.cro_event_17_triggered,
    t3.cro_event_18_triggered,
    t3.cro_event_19_triggered,
    t3.cro_event_20_triggered,

    t4.user_type,

    t5.channel_grouping_session
from _1002 t1
left join
    _1003 t2 on t1.user_pseudo_id = t2.user_pseudo_id and t1.session_id = t2.session_id
left join
    _1004 t3 on t1.user_pseudo_id = t3.user_pseudo_id and t1.session_id = t3.session_id
left join 
    _1005 t4 on t1.user_pseudo_id = t4.user_pseudo_id and t1.session_id = t4.session_id
left join
    _1006 t5 on t1.traffic_source = t5.traffic_source and t1.traffic_medium = t5.traffic_medium
