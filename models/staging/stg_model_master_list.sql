select
    Model as model_name,
    Make as make,
    Category as category,
    Year as year,
    DisplayName as display_name,
    IsInProduction as is_in_production,
    FirstProducedModel as first_produced,
    USModelID as model_id_us,
    BRModelID as model_id_br,
    DisplayPriceTagUSD as price_usd

from {{ source('second_dataset', 'model_master_list') }}
