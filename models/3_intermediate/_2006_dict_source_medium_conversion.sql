WITH traffic_source_medium AS (
    SELECT DISTINCT
        traffic_source,
        traffic_medium,
    FROM {{ref('_2001_6_events_join')}}
),

final AS (
    SELECT
        traffic_source, 
        traffic_medium,
        CASE 
            WHEN traffic_source LIKE '%email%' THEN 'Email'
            WHEN traffic_source IS NULL AND COALESCE(traffic_medium, '(not set)') IN ('(not set)', 'social-paid') THEN 
                CASE WHEN traffic_medium = 'social-paid' THEN 'Paid Social' ELSE 'Direct' END
            WHEN traffic_source = '(direct)' AND traffic_medium = '(none)' THEN 'Direct'
            WHEN 
                regexp_contains(traffic_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart') OR
                regexp_contains(traffic_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex') OR 
                regexp_contains(traffic_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp|meta') OR
                regexp_contains(traffic_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch')
                AND regexp_contains(traffic_medium,'^(.*cp.*|ppc|paid.*)$') THEN 
                CASE 
                    WHEN regexp_contains(traffic_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart') THEN 'Paid Shopping'
                    WHEN regexp_contains(traffic_source,'baidu|bing|duckduckgo|ecosia|google|yahoo|yandex') THEN 'Paid Search'
                    WHEN regexp_contains(traffic_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp|meta') THEN 'Paid Social'
                    ELSE 'Paid Video'
                END
            WHEN traffic_medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm') THEN 'Display'
            WHEN 
                regexp_contains(traffic_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart') OR
                regexp_contains(traffic_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp|meta') OR
                regexp_contains(traffic_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch')
                OR traffic_medium IN ('social','social-network','social-media','sm','social network','social media', 'organic') THEN 
                CASE 
                    WHEN regexp_contains(traffic_source,'alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart') THEN 'Organic Shopping'
                    WHEN regexp_contains(traffic_source,'badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp|meta') OR 
                         traffic_medium IN ('social','social-network','social-media','sm','social network','social media') THEN 'Organic Social'
                    WHEN regexp_contains(traffic_source,'dailymotion|disneyplus|netflix|youtube|vimeo|twitch') OR 
                         regexp_contains(traffic_medium,'^(.*video.*)$') THEN 'Organic Video'
                    ELSE 'Organic Search'
                END
            WHEN regexp_contains(traffic_medium,'email|e-mail|e_mail|e mail') THEN 'Email'
            WHEN traffic_medium = 'affiliate' THEN 'Affiliates'
            WHEN traffic_medium = 'referral' THEN 'Referral'
            WHEN traffic_medium = 'audio' THEN 'Audio'
            WHEN traffic_medium = 'sms' THEN 'SMS'
            WHEN regexp_contains(traffic_medium,'mobile|notification|%push') THEN 'Mobile Push Notifications'
            ELSE 'Unassigned'
        END AS channel_grouping_session 
    FROM
        traffic_source_medium
)

SELECT * FROM final
ORDER BY traffic_source