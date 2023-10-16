{{ config(materialized='table') }}

WITH
    _1002 AS (SELECT * FROM {{ ref("_1001_1_pers-vwo_lib") }}),
    _1003 AS (SELECT * FROM {{ ref("_1001_2_events_not-vwo-or-sqzly") }})

SELECT
    _1002.*,
    _1003.*
 FROM
    _1002
LEFT JOIN
    _1003
ON
    _1002.user_pseudo_id_1 = _1003.user_pseudo_id
    AND _1002.session_id_1 = _1003.session_id