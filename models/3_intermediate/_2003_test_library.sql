with _0001 as (select * from {{ ref("_2001_6_events_join") }}),

_vwo AS (
  SELECT DISTINCT
    CAST(user_pseudo_id AS STRING) AS user_pseudo_id,
    CAST(session_id AS STRING) AS session_id,
    CAST(experiment_id AS STRING) AS experiment_id,
    CAST(experiment_name AS STRING) AS experiment_name,
    CAST(variant_id AS STRING) AS variant_id,
    CAST(variant_name AS STRING) AS variant_name,
    CASE 
      WHEN LOWER(variant_name) LIKE '%control%' THEN 'Control'
      WHEN variant_name != '%control%' THEN 'Variant'
    END AS variant_type
  FROM _0001
  WHERE 
    LOWER(event_name) LIKE '%experience_impression%' 
    AND experiment_id IS NOT NULL 
    AND variant_name IS NOT NULL
  ORDER BY user_pseudo_id
)

SELECT * FROM _vwo