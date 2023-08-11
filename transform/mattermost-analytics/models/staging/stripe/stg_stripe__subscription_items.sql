
with source as (

    select * from {{ source('stripe', 'subscription_items') }}

),

renamed as (

    select
        -- Primary key
        id as subscription_item_id,

        -- Foreign keys
        subscription as subscription_id,
        plan:product::varchar as product_id,

        -- Data
        created as created_at,
        -- Column metadata only contains a `cloud` field with boolean value so far. This value seems to be inconsistent
        -- with plan name. Ignoring this column for now.
        -- metadata,
        -- Column object has always value `subscription_item`, ignoring column.
        -- object,
        -- Plan is a json column. Keeping the more meaningful columns that seem to have value in all rows. More can
        -- be added whenever needed.
        plan:"interval"::varchar as plan_interval,
        plan:"interval_count"::varchar as plan_interval_count,  -- Always 1 for now

        -- Price is a json column. Values should be taken from invoices instead, thus ignoring for now.
        -- price,
        quantity
        -- Column tax rates has always value `[]`, ignoring column.
        -- tax_rates

        -- Stitch columns omitted

    from source

)

select * from renamed
where
    -- Known problematic cases
    -- On prem subscriptions with more than 2 subscription items
    subscription_id not in (
        'sub_IIhi2F9b4KvQof',
        'sub_IIhmz3ZpMrAlV2'
    )
