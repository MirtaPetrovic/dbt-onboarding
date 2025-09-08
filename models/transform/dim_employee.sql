with store1 as (

    select distinct
        src_employee_id,
        first_name,
        last_name,
        phone_number,
        position,
        hired_on,
        employee_address as address,
        country_code
    from {{ ref('stg_store_tracking') }}

),

deduped as (
    select
        row_number() over(order by src_employee_id) as employee_id,
        *
    from store1
)

select * from deduped


