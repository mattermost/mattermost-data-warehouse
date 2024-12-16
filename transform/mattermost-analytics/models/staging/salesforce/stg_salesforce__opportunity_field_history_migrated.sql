-- Copy of the opportunityfieldhistory table with the latest version of each record.
-- To be used only for migrated hightouch syncs.
SELECT
    DISTINCT o.id as sfid,
             o.*,
             o.opportunity_id as opportunity_id,
             coalesce(NEWVALUE__BO::VARCHAR,NEWVALUE__DE::VARCHAR,NEWVALUE__FL::VARCHAR,NEWVALUE__ST::VARCHAR) as newvalue,
             coalesce(OLDVALUE__BO::VARCHAR,OLDVALUE__DE::VARCHAR,OLDVALUE__FL::VARCHAR,OLDVALUE__ST::VARCHAR) as oldvalue
FROM {{ source('salesforce', 'opportunityfieldhistory') }} o
INNER JOIN (
    SELECT id,
            MAX(_sdc_sequence) AS seq,
            MAX(_sdc_batched_at) AS batch
    FROM {{ source('salesforce', 'opportunityfieldhistory') }}
    GROUP BY id) oo
ON o.id = oo.id
    AND o._sdc_sequence = oo.seq
    AND o._sdc_batched_at = oo.batch
WHERE NOT isdeleted