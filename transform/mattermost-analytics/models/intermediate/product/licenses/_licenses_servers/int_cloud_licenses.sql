with stripe_licenses as (
    select distinct cws_installation as installation_id
        , c.portal_customer_id as customer_id
        , c.email as email
        , c.name as company_name
        , s.edition as edition
        , 'Stripe' as source
    from {{ ref('stg_stripe__subscriptions')}}  s 
    join {{ ref('stg_stripe__customers')}}  c on s.customer_id = c.customer_id
    where cws_installation is not null
) select * from stripe_licenses