{{config({
    "materialized": "table",
    "schema": "bizops"
  })
}}

WITH orgm_duplicate_errors AS (
    SELECT 'account' AS table_name, count(sfid) AS count, count(distinct sfid) AS count_distinct
    FROM {{ ref('account')}}
    GROUP BY 1

    UNION ALL

    SELECT 'opportunity' AS table_name, count(sfid) AS count, count(distinct sfid) AS count_distinct
    FROM {{ ref('opportunity')}}
    GROUP BY 1

    UNION ALL

    SELECT 'opportunitylineitem' AS table_name, count(sfid) AS count, count(distinct sfid) AS count_distinct
    FROM {{ ref('opportunitylineitem')}}
    GROUP BY 1

    UNION ALL

    SELECT 'contact' AS table_name, count(sfid) AS count, count(distinct sfid) AS count_distinct
    FROM {{ ref('contact')}}
    GROUP BY 1

    UNION ALL

    SELECT 'product2' AS table_name, count(sfid) AS count, count(distinct sfid) AS count_distinct
    FROM {{ ref('product2')}}
    GROUP BY 1

    UNION ALL

    SELECT 'billing_entity__c' AS table_name, count(sfid) AS count, count(distinct sfid) AS count_distinct
    FROM {{ ref('billing_entity__c')}}
    GROUP BY 1

    UNION ALL

    SELECT 'opportunitycontactrole' AS table_name, count(sfid) AS count, count(distinct sfid) AS count_distinct
    FROM {{ ref('opportunitycontactrole')}}
    GROUP BY 1
        
    UNION ALL

    SELECT 'user' AS table_name, count(sfid) AS count, count(distinct sfid) AS count_distinct
    FROM {{ ref('user')}}
    GROUP BY 1

    UNION ALL

    SELECT 'lead' AS table_name, count(sfid) AS count, count(distinct sfid) AS count_distinct
    FROM {{ ref('lead')}}
    GROUP BY 1

    UNION ALL

    SELECT 'campaign' AS table_name, count(sfid) AS count, count(distinct sfid) AS count_distinct
    FROM {{ ref('campaign')}}
    GROUP BY 1

    UNION ALL

    SELECT 'campaignmember' AS table_name, count(sfid) AS count, count(distinct sfid) AS count_distinct
    FROM {{ ref('campaignmember')}}
    GROUP BY 1

    UNION ALL

    SELECT 'campaignmember' AS table_name, count(sfid) AS count, count(distinct sfid) AS count_distinct
    FROM {{ ref('campaignmember')}}
    GROUP BY 1
), snowflake_data_checks AS (
    SELECT *
    FROM orgm_duplicate_errors
    WHERE count <> count_distinct
)

SELECT * FROM snowflake_data_checks