WITH exp_event_joined AS (
    SELECT 
        e.*,
        j.experiment_id,
        j.experiment_name,
        j.variant_id,
        j.variant_name
    FROM
        {{ ref('_2001_2_exp_lib') }} j
    JOIN
        {{ ref('_2001_4_event_lib') }} e
    ON
        CAST(j.event_date AS DATE) = e.event_date AND
        j.session_id = e.session_id
)

SELECT * FROM exp_event_joined