with dates as (

    -- generiramo raspon datuma
    select
        day as date_day
    from unnest(
        generate_date_array('2025-01-01', '2030-12-31', interval 1 day)
    ) as day

),

final as (
    select
        cast(format_date('%Y%m%d', date_day) as int64) as date_id,
        date_day as full_date,
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        format_date('%B', date_day) as month_name,
        extract(day from date_day) as day,
        extract(dayofweek from date_day) as day_of_week, -- 1=Sunday u BQ
        case when extract(dayofweek from date_day) in (1,7) then true else false end as is_weekend,
        extract(week from date_day) as week_of_year
    from dates
)

select * from final
