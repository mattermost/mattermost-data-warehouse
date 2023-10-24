with stripe_licenses as (
    select distinct s.cws_installation as installation_id
        , c.portal_customer_id as customer_id
        , c.email as email
        , c.name as company_name
        , s.edition as license_name
        , 'Stripe' as source
    from {{ ref('stg_stripe__subscriptions')}}  s 
    join {{ ref('stg_stripe__customers')}}  c on s.customer_id = c.customer_id
    where s.cws_installation is not null
) select * from stripe_licenses