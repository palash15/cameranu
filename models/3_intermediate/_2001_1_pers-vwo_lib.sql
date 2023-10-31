with events as (select * from {{ref("_1001_bigquery_primera")}}),

clean_events as (
  SELECT DISTINCT
    PARSE_DATE("%Y%m%d", event_date) as event_date_1,
    user_pseudo_id AS user_pseudo_id_1,
    (select value.int_value from unnest(event_params) where key = "ga_session_id") as session_id_1,
    event_name,
    (select value.string_value from unnest(event_params) where key = "vwo_campaign_name") as experiment_id,
    (select value.string_value from unnest(event_params) where key = "vwo_variation_name") as variant_id,
    (select value.string_value from unnest(event_params) where key = "vwo_campaign_name") as experiment_name,
    (select value.string_value from unnest(event_params) where key = "vwo_variation_name") as variant_name
  FROM events
)

SELECT
    event_date_1,
    user_pseudo_id_1,
    session_id_1,
    experiment_id,
    variant_id,
    experiment_name,
    variant_name
FROM clean_events