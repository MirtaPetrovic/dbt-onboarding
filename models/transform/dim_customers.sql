with us as (

    select
        cast(src_customer_id as int64) as src_customer_id,
        customer_first_name as first_name,
        customer_last_name as last_name,
        country,
        address,
        signup_date,
        case 
        when src_customer_id = referred_by then null
            else referred_by
        end as referred_by
    from {{ ref('stg_us_sales') }}
    where src_customer_id is not null

),

br as (

    select
        cast(src_customer_id as int64) as src_customer_id,
        split(customer_name, ' ')[safe_offset(0)] as first_name,
        split(customer_name, ' ')[safe_offset(1)] as last_name,
        country,
        cast(null as string) as address,
        signup_date,
        cast(null as string) as referred_by
    from {{ ref('stg_br_sales') }}
    where src_customer_id is not null

),

unioned as (

    select * from us
    union all
    select * from br
),

deduped as (
    select 
        row_number() over(order by src_customer_id) as customer_id,
        *
    from unioned
)

select * from deduped
