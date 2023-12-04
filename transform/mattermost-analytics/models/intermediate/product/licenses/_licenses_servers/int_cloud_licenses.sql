select s.cws_installation as installation_id
        , c.portal_customer_id as customer_id
        , c.email as email
        , c.name as company_name
        , coalesce(s.edition, p.name) as license_name
        , 'Stripe' as source
    from {{ ref('stg_stripe__subscriptions')}}  s 
    join {{ ref('stg_stripe__customers')}}  c on s.customer_id = c.customer_id
    left join {{ ref('stg_stripe__products')}} p on coalesce(s.product_id, s.current_product_id) = p.product_id
    where s.cws_installation is not null
    qualify row_number() over (partition by cws_installation order by current_period_end_at desc) = 1
