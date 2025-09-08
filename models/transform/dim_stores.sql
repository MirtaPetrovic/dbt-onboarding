with store1 as (

    select distinct
        src_store_id,
        store_name,
        address,
        country_code,
        post_code
    from {{ ref('stg_store_tracking') }}

),

deduped as (
    select
        row_number() over(order by src_store_id) as store_id,
        *
    from store1
)

select * from deduped


