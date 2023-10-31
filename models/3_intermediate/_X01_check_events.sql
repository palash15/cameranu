WITH 
    vwo AS (
        SELECT
            session_id,
            event_date,
            user_pseudo_id,
            personalization_id,
            personalization_name,
            personalization_variant_id,
            personalization_variant_name,
            vwo_experiment_id,
            vwo_experiment_name,
            vwo_variant_id,
            vwo_variant_name
        FROM
            {{ ref('_2001_3_vwo_lib') }}
    ),
    exp AS (
        SELECT
            session_id,
            experiment_name,
            experiment_id,
            variant_name,
            variant_id
        FROM
            {{ ref('_2001_2_exp_lib') }}
    ),

 _join AS(   
SELECT
    vwo.session_id,
    vwo.event_date,
    vwo.user_pseudo_id,
    vwo.personalization_id,
    vwo.personalization_name,
    vwo.personalization_variant_id,
    vwo.personalization_variant_name,
    exp.experiment_name,
    exp.experiment_id,
    exp.variant_name,
    exp.variant_id,
    vwo.vwo_experiment_id,
    vwo.vwo_experiment_name,
    vwo.vwo_variant_id,
    vwo.vwo_variant_name
FROM
    vwo
LEFT JOIN
    exp
ON
    vwo.session_id = exp.session_id
 )

 SELECT DISTINCT * FROM _join