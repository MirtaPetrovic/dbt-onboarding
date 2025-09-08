with base as (

    select
        Cabecalho.TransacaoID as src_sale_id,
        Cabecalho.TransacaoTempo as sale_ts,
        Cabecalho.LojaID as src_store_id,
        
        Cabecalho.Cliente.ClienteID as src_customer_id,
        Cabecalho.Cliente.ClienteNome as customer_name,
        Cabecalho.Cliente.Localizacao as address,

        "BR" as country,              
        date(Cabecalho.TransacaoTempo) as signup_date, 
        null as referred_by, 
        
        Modelo.ModeloID as src_model_id,

        case 
            when Cabecalho.Moeda = 'USD' then Modelo.Preco
            when Cabecalho.Moeda = 'BRL' then Modelo.Preco * 0.20
        end as model_price,
        
        Cabecalho.Cartao.Numero as card_number,
        Cabecalho.Cartao.DataDeValidade as card_expires,
        
        null as employee_name,
        
        date(Cabecalho.TransacaoTempo) as sale_date

    from {{ source('source_dataset', 'sales_br') }}

),

with_valid as (
    select *,
        case 
            when PARSE_DATE('%m/%y', card_expires) >= sale_date then true
            else false
        end as is_card_valid
    from base
)

select *
from with_valid
where is_card_valid = true
