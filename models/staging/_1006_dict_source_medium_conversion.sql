with traffic_source_medium as (
    select distinct
        traffic_source,
        traffic_medium,
        from {{ref('_1001_events_transform')}}
),

final AS (
    SELECT
        traffic_source, 
        traffic_medium,
        CASE 
            WHEN traffic_source IS NULL AND (traffic_medium IS NULL OR traffic_medium = '(not set)') THEN 'Direct'
            WHEN traffic_source IS NULL AND (traffic_medium IS NULL OR traffic_medium = 'social-paid') THEN 'Paid Social'
            WHEN (traffic_source = '(direct)' AND traffic_medium = '(none)') THEN 'Direct'
            WHEN regexp_contains(traffic_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart')
                AND regexp_contains(traffic_medium, '^(.*cp.*|ppc|paid.*)$') THEN 'Paid Shopping'
            WHEN regexp_contains(traffic_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
                AND regexp_contains(traffic_medium,'^(.*cp.*|ppc|paid.*)$') THEN 'Paid Search'
            WHEN regexp_contains(traffic_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp|meta')
                AND regexp_contains(traffic_medium,'^(.*cp.*|ppc|paid.*)$') THEN 'Paid Social'
            WHEN regexp_contains(traffic_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
                AND regexp_contains(traffic_medium,'^(.*cp.*|ppc|paid.*)$') THEN 'Paid Video'
            WHEN regexp_contains(traffic_medium,'^(.*cp.*|ppc|paid.*)$') THEN 'Paid Other'
            WHEN traffic_medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm') THEN 'Display'
            WHEN regexp_contains(traffic_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart') THEN 'Organic Shopping'
            WHEN regexp_contains(traffic_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp')
                OR traffic_medium IN ('social','social-network','social-media','sm','social network','social media') THEN 'Organic Social'
            WHEN regexp_contains(traffic_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch|vimeo|youtube')
                OR regexp_contains(traffic_medium,'^(.*video.*)$') THEN 'Organic Video'
            WHEN regexp_contains(traffic_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex')
                OR traffic_medium = 'organic' THEN 'Organic Search'
            WHEN regexp_contains(traffic_source,'email|e-mail|e_mail|e mail')
                OR regexp_contains(traffic_medium,'email|e-mail|e_mail|e mail') THEN 'Email'
            WHEN traffic_medium = 'affiliate' THEN 'Affiliates'
            WHEN traffic_medium = 'referral' THEN 'Referral'
            WHEN traffic_medium = 'audio' THEN 'Audio'
            WHEN traffic_medium = 'sms' THEN 'SMS'
            WHEN traffic_medium LIKE '%push'
                OR regexp_contains(traffic_medium,'mobile|notification') THEN 'Mobile Push Notifications'
            ELSE 'Unassigned'
        END AS channel_grouping_session 
    FROM
        traffic_source_medium
)

SELECT * FROM final
ORDER BY traffic_source