{{ config(
    materialized='table',
    partition_by={
       "field": "event_date",
       "data_type": "date"
    },
    cluster_by=["event_name", "event_category", "device", "browser"]
) }}

WITH
    _1002 AS (SELECT * FROM {{ ref("_1002_users_events") }}),
    _1003 AS (SELECT * FROM {{ ref("_1008_combined_library") }}),
    _1005 AS (SELECT * FROM {{ ref("_1005_user_type") }}),
    _1006 AS (SELECT * FROM {{ ref("_1006_dict_source_medium_conversion") }}),

    _2000 AS (
        SELECT
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

            t2.experiment_id,
            t2.experiment_name,
            t2.variant_id,
            t2.variant_name,
            t2.variant_type,

            t4.user_type,

            t5.channel_grouping_session
        FROM _1002 t1
            LEFT JOIN _1003 t2 ON 
        CAST(t1.user_pseudo_id AS STRING) = CAST(t2.user_pseudo_id AS STRING) 
        AND CAST(t1.session_id AS STRING) = CAST(t2.session_id AS STRING)
    LEFT JOIN _1005 t4 ON 
        CAST(t1.user_pseudo_id AS STRING) = CAST(t4.user_pseudo_id AS STRING) 
        AND CAST(t1.session_id AS STRING) = CAST(t4.session_id AS STRING)
    LEFT JOIN _1006 t5 ON 
        CAST(t1.traffic_source AS STRING) = CAST(t5.traffic_source AS STRING) 
        AND CAST(t1.traffic_medium AS STRING) = CAST(t5.traffic_medium AS STRING)
)

SELECT * FROM _2000