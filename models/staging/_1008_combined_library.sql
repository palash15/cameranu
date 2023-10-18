WITH _vwo AS (
    SELECT * 
    FROM {{ ref('_1003_test_library') }}
),

_union AS (
    SELECT
        user_pseudo_id,
        session_id,
        experiment_id,
        experiment_name,
        variant_id,
        variant_name,
        variant_type
    FROM _vwo
)

SELECT * FROM _union