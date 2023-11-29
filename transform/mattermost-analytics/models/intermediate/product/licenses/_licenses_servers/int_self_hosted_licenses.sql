with stripe_licenses as (
    select distinct license_id
        , c.portal_customer_id as customer_id
        , c.email as email
        , c.name as company_name
        , coalesce(s.edition, p.name) as license_name
        , 'Stripe' as source
    from {{ ref('stg_stripe__subscriptions')}}  s 
    join {{ ref('stg_stripe__customers')}}  c on s.customer_id = c.customer_id
    left join {{ ref('stg_stripe__products')}} p on coalesce(s.product_id, s.current_product_id) = p.product_id
    where license_id is not null
), cws_licenses as (
    select distinct cl.license_id
        , cl.customer_id as customer_id
        , cl.customer_email as email
        , cl.company_name as company_name
        , coalesce (p.name, cl.sku_short_name) as license_name
        , 'CWS' as source
    from {{ ref('stg_cws__license') }} cl
    left join {{ ref('stg_stripe__products')}} p on cl.stripe_product_id = p.product_id
    where not exists (select 1 from stripe_licenses sl where cl.license_id = sl.license_id)
), legacy_licenses as (
    select distinct license_id
        , customer_id 
        , contact_email as email
        , company_name 
        , null as license_name
        , 'Legacy' as source
    from {{ ref('stg_licenses__licenses')}} ll
    where not exists (select 1 from stripe_licenses sl where ll.license_id = sl.license_id) 
    and not exists ((select 1 from cws_licenses cl where ll.license_id = cl.license_id))
) select * from cws_licenses
    union
    select * from stripe_licenses
    union 
    select * from legacy_licenses