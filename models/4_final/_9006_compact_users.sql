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
transaction_id,
revenue
FROM {{ ref('_9001_device_data') }}
)
 
SELECT distinct event_date, experiment_name,variant_name, user_type,channel_grouping_session, device, browser,
                    string_agg(user_pseudo_id) as user_pseudo_ids,
                    string_agg(ifnull(transaction_id,'0')) as transaction_ids,
                    string_agg(ifnull(cast(revenue as string),'0')) as revenues
FROM _9006
GROUP BY event_date, experiment_name, variant_name, device, user_type, channel_grouping_session, browser            