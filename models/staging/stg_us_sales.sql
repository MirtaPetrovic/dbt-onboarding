with base as (

    select
        t.transaction_id as src_sale_id,
        t.transaction_time as sale_ts,
        t.store as src_store_id,
        
        t.registered_customer.registered_customer_id as src_customer_id,
        t.registered_customer.first_name as customer_first_name,
        t.registered_customer.last_name as customer_last_name,
        concat(t.registered_customer.first_name, ' ', t.registered_customer.last_name) as customer_name,
        t.registered_customer.gender as gender,
        t.registered_customer.registered_since as signup_date,
        t.registered_customer.address.country_code as country,
        t.registered_customer.address.address as address,
        t.registered_customer.contact.phone_number as phone_number,
        case 
            when t.registered_customer.referred_by = t.registered_customer.registered_customer_id 
            then null
            else t.registered_customer.referred_by
        end as referred_by,
        
        m.line_num,
        m.model as src_model_id,
        m.model_price,
        
        t.payment.card_information.card_number as card_number,
        t.payment.card_information.card_expires as card_expires,
        t.payment.total.payment as total_amount_source,
        t.payment.total.currency as currency,
        
        t.employee as employee_name,
        date(t.transaction_time) as sale_date

    from {{ source('source_dataset', 'sales_us') }} t,
    unnest(t.models_purchased) as m

),

agg as (
    select
        src_sale_id,
        sum(model_price) as calculated_total
    from base
    group by src_sale_id
),

joined as (
    select
        b.*,
        a.calculated_total,
        case when abs(b.total_amount_source - a.calculated_total) < 0.01 then true else false end as is_total_consistent,
        case 
            when PARSE_DATE('%m/%y', b.card_expires) >= b.sale_date then true 
            else false 
        end as is_card_valid
    from base b
    join agg a using (src_sale_id)
)

select *
from joined
where is_total_consistent = true
  and is_card_valid = true 
