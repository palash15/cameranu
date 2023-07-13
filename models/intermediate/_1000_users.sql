with
    _1001 as (select * from {{ ref("_1001_users_events") }}),
    _1002 as (select * from {{ ref("_1002_test_library") }}),
    _1003 as (select * from {{ ref("_1003_dict_event_triggered") }}),
    _1005 as (select * from {{ ref("_1005_dict_source_medium_conversion") }})

select
    t1.user_pseudo_id,
    t1.session_id,
    t1.event_date,
    t1.event_name,
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

    t3.addtocart_triggered,
    t3.purchase_triggered,
    t3.viewitem_triggered,
    t3.viewcart_triggered,
    t3.checkout_triggered,
    t3.pdp_seen,
    t3.collection_page_seen,

    t5.channel_grouping_session
from _1001 t1
left join
    _1002 t2 on t1.user_pseudo_id = t2.user_pseudo_id and t1.session_id = t2.session_id
left join
    _1003 t3 on t1.user_pseudo_id = t3.user_pseudo_id and t1.session_id = t3.session_id
-- left join _1004 t4 on t1.user_pseudo_id = t4.user_pseudo_id and t1.session_id =
-- t4.session_id
left join
    _1005 t5 on t1.traffic_source = t5.traffic_source and t1.traffic_medium = t5.traffic_medium
