with source as (
    select * from {{ source('geolite', 'country_locations') }}
),
renamed as (

    select
        geoname_id
        , locale_code
        , continent_code
        , continent_name
        , country_iso_code
        , country_name
        , case
            when is_in_european_union = 1 then true
            else false
        end as is_in_european_union
    from source
)

select * from renamed
