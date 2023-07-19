with _0001 as (select * from {{ ref("_1001_events_transform") }})

select distinct
    user_pseudo_id,
    session_id,
    vwo_uuid,
    vwo_campaign_id as experiment_id,
    vwo_campaign_name as experiment_name,
    vwo_variant_id as variant_id,
    vwo_variant_name as variant_name,
    case when lower(vwo_variant_name) like '%control%' then 'Control'
         when vwo_variant_name is not null then 'Variant' 
    end as variant_type
         
from _0001
where lower(event_name) like '%vwo%' and vwo_campaign_name is not null and vwo_variant_name is not null
order by user_pseudo_id
