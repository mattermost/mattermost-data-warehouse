{{config({
    "materialized": 'table',
    "schema": "blp",
    "tags":'hourly'
  })
}}


with account_mapping as (
  SELECT 
      elm.account_sfid
    , a.name as account_name
    , elm.licenseid as license_id
    , elm.opportunity_sfid
    , elm.company
    , elm.contact_sfid
    , elm.edition
  FROM (
        SELECT
            COALESCE(elm.account_sfid, lo.account_sfid)         AS account_sfid
          , COALESCE(elm.opportunity_sfid, lo.opportunity_sfid) AS opportunity_sfid
          , COALESCE(trim(elm.licenseid), trim(lo.licenseid))   AS licenseid
          , COALESCE(trim(elm.company), trim(lo.company))       AS company
          , COALESCE(trim(lo.contact_sfid), NULL)       AS contact_sfid
          , lo.edition AS edition
        FROM {{ ref('enterprise_license_mapping') }} elm
        FULL OUTER JOIN {{ ref('license_overview') }} lo
          ON trim(elm.licenseid) = trim(lo.licenseid)
        GROUP BY 1, 2, 3, 4, 5, 6
      ) elm
  LEFT JOIN {{ ref( 'account') }} a
      ON elm.account_sfid = a.sfid
  GROUP BY 1, 2, 3, 4, 5, 6, 7
) 
select * from account_mapping