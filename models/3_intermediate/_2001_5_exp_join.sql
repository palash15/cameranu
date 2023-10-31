WITH 
    base AS (
        SELECT DISTINCT
            session_id,
            event_date
        FROM {{ ref('_2001_1_squeezely_lib') }}
        UNION ALL
        SELECT DISTINCT
            session_id,
            event_date
        FROM {{ ref('_2001_2_exp_lib') }}
        UNION ALL
        SELECT DISTINCT
            session_id,
            event_date
        FROM {{ ref('_2001_3_vwo_lib') }}
    ),
    
    joined AS (
        SELECT
            base.session_id,
            base.event_date,
            COALESCE(
                CAST(s.personalization_id AS STRING), 
                CAST(e.personalization_id AS STRING), 
                CAST(v.personalization_id AS STRING)
            ) AS personalization_id,
            COALESCE(s.personalization_name, e.personalization_name, v.personalization_name) AS personalization_name,
            COALESCE(
                CAST(s.personalization_variant_id AS STRING), 
                CAST(e.personalization_variant_id AS STRING), 
                CAST(v.personalization_variant_id AS STRING)
            ) AS personalization_variant_id,
            COALESCE(s.personalization_variant_name, e.personalization_variant_name, v.personalization_variant_name) AS personalization_variant_name,
            COALESCE(
                CAST(s.experiment_id AS STRING), 
                CAST(e.experiment_id AS STRING), 
                CAST(v.experiment_id AS STRING)
            ) AS experiment_id,
            COALESCE(s.experiment_name, e.experiment_name, v.experiment_name) AS experiment_name,
            COALESCE(
                CAST(s.variant_id AS STRING), 
                CAST(e.variant_id AS STRING), 
                CAST(v.variant_id AS STRING)
            ) AS variant_id,
            COALESCE(s.variant_name, e.variant_name, v.variant_name) AS variant_name,
            COALESCE(
                CAST(s.vwo_experiment_id AS STRING), 
                CAST(e.vwo_experiment_id AS STRING), 
                CAST(v.vwo_experiment_id AS STRING)
            ) AS vwo_experiment_id,
            COALESCE(s.vwo_experiment_name, e.vwo_experiment_name, v.vwo_experiment_name) AS vwo_experiment_name,
            COALESCE(
                CAST(s.vwo_variant_id AS STRING), 
                CAST(e.vwo_variant_id AS STRING), 
                CAST(v.vwo_variant_id AS STRING)
            ) AS vwo_variant_id,
            COALESCE(s.vwo_variant_name, e.vwo_variant_name, v.vwo_variant_name) AS vwo_variant_name,
            CAST(s.user_pseudo_id AS STRING) AS user_pseudo_id_s,
            CAST(e.user_pseudo_id AS STRING) AS user_pseudo_id_e,
            CAST(v.user_pseudo_id AS STRING) AS user_pseudo_id_v
        FROM
            base
        LEFT JOIN
    {{ ref('_2001_1_squeezely_lib') }} s
ON
    base.session_id = s.session_id AND 
    base.event_date = s.event_date AND 
    s.personalization_id IS NOT NULL
LEFT JOIN
    {{ ref('_2001_2_exp_lib') }} e
ON
    base.session_id = e.session_id AND 
    base.event_date = e.event_date AND 
    e.experiment_id IS NOT NULL
LEFT JOIN
    {{ ref('_2001_3_vwo_lib') }} v
ON
    base.session_id = v.session_id AND 
    base.event_date = v.event_date AND 
    v.vwo_experiment_id IS NOT NULL
    ),
    
selection AS( SELECT DISTINCT
    joined.event_date,
        IFNULL(
        joined.user_pseudo_id_s, 
        IFNULL(
            joined.user_pseudo_id_e, 
            joined.user_pseudo_id_v
        )
    ) AS user_pseudo_id,
    joined.session_id,
    joined.personalization_id,
    joined.personalization_name,
    joined.personalization_variant_id,
    joined.personalization_variant_name,
    joined.experiment_id,
    joined.experiment_name,
    joined.variant_id,
    joined.variant_name,
    joined.vwo_experiment_id,
    joined.vwo_experiment_name,
    joined.vwo_variant_id,
    joined.vwo_variant_name,
FROM 
    joined
WHERE session_id IS NOT NULL
),
  
    experiment_vwo_mapping AS (
        SELECT DISTINCT
            SAFE_CAST(SPLIT(vwo_experiment_id, ':')[SAFE_OFFSET(1)] AS INT64) AS vwo_experiment_id_int,
            vwo_experiment_id,
            vwo_experiment_name,
            vwo_variant_id,
            vwo_variant_name
        FROM {{ ref('_2001_3_vwo_lib') }}
    ),
    
    updated_selection AS (
        SELECT
            selection.event_date,
            selection.user_pseudo_id,
            selection.session_id,
            selection.personalization_id,
            selection.personalization_name,
            selection.personalization_variant_id,
            selection.personalization_variant_name,
            CASE 
                WHEN experiment_vwo_mapping.vwo_experiment_id_int IS NOT NULL THEN experiment_vwo_mapping.vwo_experiment_id
                ELSE selection.experiment_id
            END AS experiment_id,
            CASE 
                WHEN experiment_vwo_mapping.vwo_experiment_id_int IS NOT NULL THEN experiment_vwo_mapping.vwo_experiment_name
                ELSE selection.experiment_name
            END AS experiment_name,
            CASE 
                WHEN selection.variant_name = '1' THEN '1'
                WHEN SAFE_CAST(selection.variant_name AS INT64) > 1 THEN selection.variant_id
                ELSE selection.variant_id
            END AS variant_id,
            CASE 
                WHEN selection.variant_name = '1' THEN '1:control'
                WHEN SAFE_CAST(selection.variant_name AS INT64) > 1 THEN CONCAT(selection.variant_name, ':variant')
                ELSE selection.variant_name
            END AS variant_name,
        FROM 
            selection
        LEFT JOIN
            experiment_vwo_mapping
        ON
            SAFE_CAST(selection.experiment_id AS INT64) = experiment_vwo_mapping.vwo_experiment_id_int
    )

SELECT * FROM updated_selection