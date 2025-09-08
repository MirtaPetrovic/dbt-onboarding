with prod as (

    select
        model_name,
        make,
        category,
        cast(Year as string) as year,
        is_in_production,
        first_produced,
        model_id_us,
        model_id_br,
        cast(price_usd as numeric) as price
    from {{ ref('stg_model_master_list') }}

),

deduped as (
    select
        row_number() over (order by model_name) as model_id,
        *
    from prod
)

select * from deduped

