with events as (select * from {{ref("_0001_bigquery_paardekooper")}}),

clean_events as (
  SELECT 
    PARSE_DATE("%Y%m%d", event_date) as event_date_1,
    user_pseudo_id AS user_pseudo_id_1,
    (select value.int_value from unnest(event_params) where key = "ga_session_id") as session_id_1,
    event_name,
    (select value.string_value from unnest(event_params) where key = "exp_variant_string") as experiment_information,
    (select value.int_value from unnest(event_params) where key = "personalization_id") as personalization_id,
    (select value.string_value from unnest(event_params) where key = "personalization_name") as personalization_name,
    (select value.int_value from unnest(event_params) where key = "personalization_variant_id") as personalization_variant_id,
    (select value.string_value from unnest(event_params) where key = "variant_name") as personalization_variant_name,
  FROM events
),

selection AS (SELECT DISTINCT
    event_date_1,
    user_pseudo_id_1,
    session_id_1,
    experiment_information,
    personalization_id,
    personalization_name,
    personalization_variant_id,
    personalization_variant_name
FROM clean_events
),

split_experiment AS (
    SELECT 
        event_date_1,
        user_pseudo_id_1,
        session_id_1,
        experiment_information,

        -- Extracting experiment name and ID
        SPLIT(experiment_information, '-')[OFFSET(1)] AS experiment_name,
        SPLIT(experiment_information, '-')[OFFSET(1)] AS experiment_id,

        -- Extracting variant name and ID
        SPLIT(experiment_information, '-')[OFFSET(2)] AS variant_name,
        SPLIT(experiment_information, '-')[OFFSET(2)] AS variant_id,

        personalization_id,
        personalization_name,
        personalization_variant_id,
        personalization_variant_name

    FROM selection
    WHERE session_id_1 IS NOT NULL
)

SELECT * FROM split_experiment