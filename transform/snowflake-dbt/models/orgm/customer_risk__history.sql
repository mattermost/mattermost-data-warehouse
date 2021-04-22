{{
    config(
        {
            "materialized": "table",
            "schema": "orgm"
        }
    )
}}

SELECT DISTINCT o.id as sfid, o.*, coalesce(NEWVALUE__FL::VARCHAR,NEWVALUE__ST::VARCHAR) as newvalue, coalesce(OLDVALUE__FL::VARCHAR,OLDVALUE__ST::VARCHAR) as oldvalue
FROM {{ source('orgm_raw','customer_risk__history') }} o
INNER JOIN (
    SELECT id,
            MAX(_sdc_sequence) AS seq,
            MAX(_sdc_batched_at) AS batch
    FROM {{ source('orgm_raw','customer_risk__history') }}
    GROUP BY id) oo
ON o.id = oo.id
    AND o._sdc_sequence = oo.seq
    AND o._sdc_batched_at = oo.batch
WHERE NOT isdeleted