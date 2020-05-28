{{config({
    "materialized": 'table',
    "schema": "sales"
  })
}}

SELECT 
    REPLACE(target_slug,'attain_new_and_exp_by_segment_by_mo_','') AS employee_number,
    user.name,
    nn_target,
    nn_actual,
    nn_tva,
    nn_open_max,
    nn_open_weighted,
    nn_commit,
    nn_best_case,
    nn_pipeline,
    nn_omitted
FROM {{ ref('tva_attain_new_and_exp_by_rep_by_qtr') }}
LEFT JOIN {{ source('orgm','user') }} ON REPLACE(target_slug,'attain_new_and_exp_by_segment_by_mo_','') = user.employeenumber
LEFT JOIN {{ source('sales','commit_rep') }}