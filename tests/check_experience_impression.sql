-- tests/check_experience_impression.sql

WITH source_data AS (
    SELECT
        COUNT(*) AS row_count
    FROM
        {{ ref('_0001_bigquery_primera') }}
    WHERE
        event_name = 'experience_impression'
)

SELECT 
    row_count
FROM 
    source_data
WHERE 
    row_count = 0