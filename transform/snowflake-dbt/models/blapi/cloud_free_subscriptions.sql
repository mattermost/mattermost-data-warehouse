{{config({
    "schema": "blapi",
    "unique_key":"id",
    "tags":["hourly","blapi"]
  })
}}
-- cloud subscriptions that do not need any payment i.e cloud starter and cloud enterprise trial(30 days)
with subscriptions AS (
    SELECT
        subscriptions.*,
        products.name,
        INITCAP(SPLIT_PART(replace(cws_dns, '-', ' '), '.', 1)) as company,
        ROW_NUMBER() OVER (PARTITION BY subscriptions.customer ORDER BY subscriptions.created DESC) as row_num
    FROM {{ source('stripe', 'subscriptions')}} 
    JOIN {{ source('stripe', 'products') }} on products.id = coalesce(subscriptions.plan:"product"::varchar, subscriptions.metadata:"current_product_id"::varchar)
    WHERE subscriptions.cws_dns is not null
    AND products.name in ('Cloud Starter', 'Cloud Enterprise') 
)
SELECT * FROM subscriptions