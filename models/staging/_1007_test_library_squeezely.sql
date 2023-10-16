with _0001 as (select * from {{ ref("_1001_3_events_join") }}),

_sqzly AS (
  SELECT DISTINCT
    CAST(user_pseudo_id AS STRING) AS user_pseudo_id,
    CAST(session_id AS STRING) AS session_id,
    CAST(personalization_id AS STRING) AS experiment_id,
    CAST(personalization_name AS STRING) AS experiment_name,
    CAST(personalization_variant_id AS STRING) AS variant_id,
    CAST(personalization_variant_name AS STRING) AS variant_name,   
    CASE 
      WHEN LOWER(personalization_variant_name) LIKE '%control%' THEN 'Control'
      WHEN personalization_variant_name <> 'Control group' THEN 'Variant'
    END AS variant_type
  FROM _0001
  WHERE 
    LOWER(event_name) LIKE '%personalization%' 
    AND personalization_id IS NOT NULL 
    AND personalization_variant_name IS NOT NULL
  ORDER BY user_pseudo_id
)

SELECT * FROM _sqzly