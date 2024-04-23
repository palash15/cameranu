with _9006 as (
    SELECT distinct 
    event_date, 
    experiment_name,
    variant_name, 
    user_type,
    channel_grouping_session, 
    device, 
    browser,
    user_pseudo_id,
    COALESCE(transaction_id, '0') AS transaction_id,
    COALESCE(CAST(revenue AS STRING), '0') AS revenue,
    FROM {{ ref('_9001_device_data') }}
)
 
SELECT distinct event_date, experiment_name,variant_name, user_type,channel_grouping_session, device, browser,
                STRING_AGG(user_pseudo_id, ',') AS user_pseudo_ids,
                STRING_AGG(transaction_id, ',') AS transaction_ids,
                STRING_AGG(revenue, ',') AS revenues
FROM _9006
GROUP BY event_date, experiment_name, variant_name, device, user_type, channel_grouping_session, browser            