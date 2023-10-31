WITH exp_event_joined AS (
    SELECT 
        e.*,
        j.personalization_id,
        j.personalization_name,
        j.personalization_variant_id,
        j.personalization_variant_name,
        j.experiment_id,
        j.experiment_name,
        j.variant_id,
        j.variant_name
    FROM
        {{ ref('_2001_5_exp_join') }} j
    JOIN
        {{ ref('_2001_4_event_lib') }} e
    ON
        CAST(j.event_date AS DATE) = e.event_date AND
        j.session_id = e.session_id
)

SELECT * FROM exp_event_joined