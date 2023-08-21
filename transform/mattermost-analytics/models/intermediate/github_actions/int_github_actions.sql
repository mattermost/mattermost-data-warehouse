{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}
    with seed_file as (
    -- Github Actions data from seed file
    select
        trim(Date)
        , trim(product)
        , trim(sku)
        , trim(quantity)
        , trim(unit_type)
        , trim(price_per_unit)
        , trim(multiplier)
        , trim(owner)
        , trim(repository_slug)
        , trim(username)
        , trim(actions_workflow)
        , trim(notes)
    from
        {{ ref('github_actions') }}
)
select distinct * from seed_file