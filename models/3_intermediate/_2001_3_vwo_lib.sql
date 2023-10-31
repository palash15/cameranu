with events as (select * from {{ref("_1001_bigquery_primera")}}),

clean_events as (
  SELECT DISTINCT
    PARSE_DATE("%Y%m%d", event_date) as event_date_3,
    user_pseudo_id AS user_pseudo_id_3,
    CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id_3,
    event_name,
    (select value.string_value from unnest(event_params) where key = "vwo_campaign_name") as vwo_experiment_id,
    (select value.string_value from unnest(event_params) where key = "vwo_variation_name") as vwo_variant_id,
    (select value.string_value from unnest(event_params) where key = "vwo_campaign_name") as vwo_experiment_name,
    (select value.string_value from unnest(event_params) where key = "vwo_variation_name") as vwo_variant_name
  FROM events
),

selection AS (
    SELECT DISTINCT
        CAST(event_date_3 AS STRING) as event_date,
        CAST(user_pseudo_id_3 AS STRING) as user_pseudo_id,
        CAST(session_id_3 AS STRING) as session_id,
        CAST(NULL AS STRING) as personalization_id,
        CAST(NULL AS STRING) as personalization_name,
        CAST(NULL AS STRING) as personalization_variant_id,
        CAST(NULL AS STRING) as personalization_variant_name,
        CAST(NULL AS STRING) AS experiment_id,
        CAST(NULL AS STRING) AS experiment_name,
        CAST(NULL AS STRING) AS variant_id,
        CAST(NULL AS STRING) AS variant_name,
        CAST(vwo_experiment_id AS STRING) as vwo_experiment_id,
        CAST(vwo_variant_id AS STRING) as vwo_variant_id,
        CAST(vwo_experiment_name AS STRING) as vwo_experiment_name,
        CAST(vwo_variant_name AS STRING) as vwo_variant_name
    FROM 
        clean_events
)

SELECT * FROM selection
WHERE vwo_experiment_id IS NOT NULL