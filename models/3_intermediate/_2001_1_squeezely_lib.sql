with events as (select * from {{ref("_1001_bigquery_primera")}}),

clean_events as (
  SELECT DISTINCT
    PARSE_DATE("%Y%m%d", event_date) as event_date_1,
    user_pseudo_id AS user_pseudo_id_1,
    CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id_1,
    event_name,
    (select value.int_value from unnest(event_params) where key = "personalization_id") as personalization_id,
    (select value.string_value from unnest(event_params) where key = "personalization_name") as personalization_name,
    (select value.int_value from unnest(event_params) where key = "personalization_variant_id") as personalization_variant_id,
    (select value.string_value from unnest(event_params) where key = "variant_name") as personalization_variant_name,
  FROM events
),

selection AS (
    SELECT DISTINCT
        CAST(event_date_1 AS STRING) as event_date,
        CAST(user_pseudo_id_1 AS STRING) as user_pseudo_id,
        CAST(session_id_1 AS STRING) as session_id,
        CAST(personalization_id AS STRING) as personalization_id,
        CAST(personalization_name AS STRING) as personalization_name,
        CAST(personalization_variant_id AS STRING) as personalization_variant_id,
        CAST(personalization_variant_name AS STRING) as personalization_variant_name,
        CAST(NULL AS STRING) AS experiment_id,
        CAST(NULL AS STRING) AS experiment_name,
        CAST(NULL AS STRING) AS variant_id,
        CAST(NULL AS STRING) AS variant_name,
        CAST(NULL AS STRING) AS vwo_experiment_id,
        CAST(NULL AS STRING) AS vwo_variant_id,
        CAST(NULL AS STRING) AS vwo_experiment_name,
        CAST(NULL AS STRING) AS vwo_variant_name
    FROM 
        clean_events
)

SELECT * FROM selection
WHERE personalization_id IS NOT NULL