with events as (select * from {{ref("_1001_bigquery_primera")}}),

clean_events as (
  SELECT DISTINCT
    PARSE_DATE("%Y%m%d", event_date) as event_date_2,
    user_pseudo_id AS user_pseudo_id_2,
    CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS session_id_2,
    event_name,
    (select value.string_value from unnest(event_params) where key = "exp_variant_string") as experiment_information,
  FROM events
),

selection AS (SELECT DISTINCT
    event_date_2,
    user_pseudo_id_2,
    session_id_2,
    experiment_information,
FROM clean_events
),

split_experiment AS (
    SELECT DISTINCT
        CAST(event_date_2 AS STRING) as event_date,
        CAST(user_pseudo_id_2 AS STRING) as user_pseudo_id,
        CAST(session_id_2 AS STRING) as session_id,
        CAST(NULL AS STRING) as personalization_id,
        CAST(NULL AS STRING) as personalization_name,
        CAST(NULL AS STRING) as personalization_variant_id,
        CAST(NULL AS STRING) as personalization_variant_name,
        -- Extracting experiment name and ID
        CAST(SPLIT(experiment_information, '-')[OFFSET(1)] AS STRING) AS experiment_name,
        CAST(SPLIT(experiment_information, '-')[OFFSET(1)] AS STRING) AS experiment_id,
        -- Extracting variant name and ID
        CAST(SPLIT(experiment_information, '-')[OFFSET(2)] AS STRING) AS variant_name,
        CAST(SPLIT(experiment_information, '-')[OFFSET(2)] AS STRING) AS variant_id,
        CAST(NULL AS STRING) AS vwo_experiment_id,
        CAST(NULL AS STRING) AS vwo_experiment_name,
        CAST(NULL AS STRING) AS vwo_variant_id,
        CAST(NULL AS STRING) AS vwo_variant_name
    FROM 
        selection
)

SELECT * FROM split_experiment
WHERE experiment_name IS NOT NULL