with combined as (

    select
        cast(src_sale_id as string) as src_sale_id,
        sale_ts,
        cast(src_store_id as string) as src_store_id,
        cast(src_customer_id as string) as src_customer_id,
        cast(src_model_id as string) as src_model_id,
        cast(model_price as numeric) as model_price,
        cast(card_number as string) as card_number,
        cast(card_expires as string) as card_expires,
        cast(employee_name as string) as employee_name,
        sale_date
    from {{ ref('stg_us_sales') }}

    union all

    select
        cast(src_sale_id as string) as src_sale_id,
        sale_ts,
        cast(src_store_id as string) as src_store_id,
        cast(src_customer_id as string) as src_customer_id,
        cast(src_model_id as string) as src_model_id,
        cast(model_price as numeric) as model_price,
        cast(card_number as string) as card_number,
        cast(card_expires as string) as card_expires,
        cast(employee_name as string) as employee_name,
        sale_date
    from {{ ref('stg_br_sales') }}

),

joined as (
    select
        row_number() over(order by sale_ts, src_sale_id) as sale_id,
        c.src_sale_id,
        dc.customer_id,
        dp.model_id,
        ds.store_id,
        de.employee_id,
        c.sale_date,
        1 as quantity,
        concat('**** **** **** ', substr(c.card_number, -4)) as card_number,  
        c.model_price as total_amount,
        datetime(c.sale_ts, "UTC") as sale_ts,
        dd.date_id
    from combined c
    left join {{ ref('dim_customers') }} dc
        on cast(dc.src_customer_id as string) = c.src_customer_id
    left join {{ ref('dim_products') }} dp
        on dp.model_id_us = c.src_model_id or dp.model_id_br = c.src_model_id
    left join {{ ref('dim_stores') }} ds
        on cast(ds.src_store_id as string) = c.src_store_id
    left join {{ ref('dim_employee') }} de
        on cast(de.src_employee_id as string) = c.employee_name
    left join {{ ref('dim_date') }} dd
        on dd.full_date = c.sale_date
)

select *
from joined
where customer_id is not null
  and employee_id is not null
