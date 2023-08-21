{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}
    with seed_file as (
    -- Github Actions data from seed file
    select
        trim(date) as date
        , trim(product) as product
        , trim(sku) as sku
        , trim(quantity) as quantity
        , trim(unit_type) as unit_type
        , trim(price_per_unit) as price_per_unit
        , trim(multiplier) as multiplier
        , trim(owner) as owner
        , trim(repository_slug) as repository_slug
        , trim(username) as username
        , trim(actions_workflow) as actions_workflow
        , trim(notes) as notes
    from
        {{ ref('github_actions') }}
)
select distinct * from seed_file