with _1000 as (
    SELECT distinct event_date, experiment_name, device, variant_name, user_type,
                    channel_grouping_session, browser,
                    string_agg(distinct user_pseudo_id) as user_pseudo_ids,
                    count(distinct transaction_id) as transaction_id,
                    sum(revenue) as revenue
  FROM {{ref('_1000_users')}}
  GROUP BY EVENT_DATE, EXPERIMENT_NAME, VARIANT_NAME, DEVICE, USER_TYPE, CHANNEL_GROUPING_SESSION, BROWSER)

SELECT * FROM _1000
