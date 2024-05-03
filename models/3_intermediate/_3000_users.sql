WITH
    _1002 AS (SELECT * FROM {{ ref("_2002_users_events") }}),
    _1003 AS (SELECT * FROM {{ ref("_2008_combined_library") }}),
    _1005 AS (SELECT * FROM {{ ref("_2005_user_type") }}),
    _1006 AS (SELECT * FROM {{ ref("_2006_dict_source_medium_conversion") }}),
   
    _2000_pre_ranked AS (
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
            t1.country,
           
            t2.experiment_id,
            t2.experiment_name,
            t2.variant_id,
            t2.variant_name,
            t2.variant_type,
 
            t4.user_type,
 
            t5.channel_grouping_session,
            ROW_NUMBER() OVER (PARTITION BY t1.transaction_id ORDER BY CASE WHEN t1.event_name = 'purchase' THEN 1 ELSE 2 END) as row_rank
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
    ),
   
    _2000 AS (
        SELECT
            *,
            CASE WHEN row_rank = 1 THEN revenue ELSE 0 END AS adjusted_revenue
        FROM _2000_pre_ranked
    ),
 
compact_ids as (
    select user_pseudo_id, row_number() over (order by user_pseudo_id) as short_user_id
    FROM _2000
    group by user_pseudo_id
),
 
_1000_join as (
SELECT distinct t1.*,
       cast(t2.short_user_id as string) as short_user_id
FROM _2000 t1
left join compact_ids t2 on t1.user_pseudo_id = t2.user_pseudo_id
)
 
select * from (
    select
        short_user_id as user_pseudo_id,
        session_id,
        event_date,
        event_name,
        event_category,
        transaction_id,
        device,
        os,
        browser,
        traffic_source,
        traffic_medium,
        adjusted_revenue AS revenue,
        experiment_id,
        experiment_name,
        ifnull(REGEXP_REPLACE(experiment_name,'[^0-9 ]',''), '0') as experiment_num,
        variant_id,
        variant_name,
        variant_type,
        user_type,
        channel_grouping_session,
        case when channel_grouping_session = 'Direct' and country not in ('Netherlands', 'Belgium', 'Luxembourg') then 'exclude'
            else 'include'
        end as bot_traffic
    from _1000_join where event_category != 'other_events' and experiment_name is not null
)
where bot_traffic = 'include'
