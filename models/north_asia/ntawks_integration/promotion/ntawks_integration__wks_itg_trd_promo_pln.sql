with source as (
    select * from {{ source('ntasdl_raw','sdl_na_trade_promotion_plan') }}
),
final as(
    select 
        substring(profit_center, 0, position('-' in profit_center)) as profit_center,
        substring(profit_center, position('-' in profit_center) + 1, length(profit_center)) as profit_center_nm,
        substring(customer_channel, 0, position('-' in customer_channel)) as customer_channel,
        substring(customer_channel, position('-' in customer_channel) + 1, length(customer_channel)) as customer_channel_nm,
        case
            when position('-' in customer_hq_code) <> 0
            and length(customer_hq_code) <> 0 then substring(customer_hq_code, 0, position('-' in customer_hq_code))
            when position('-' in customer_hq_code) = 0
            and length(customer_hq_code) <> 0 then '#'
            else '#'
        end as customer_hq_code,
        case
            when position('-' in customer_hq_code) <> 0
            and length(customer_hq_code) <> 0 then substring(customer_hq_code, position('-' in customer_hq_code) + 1, length(customer_hq_code))
            when position('-' in customer_hq_code) = 0
            and length(customer_hq_code) <> 0 then customer_hq_code
            else '#'
        end as customer_hq_nm,
        country_code,
        currency_code,
        year,
        jan_tp_plan,
        feb_tp_plan,
        mar_tp_plan,
        apr_tp_plan,
        may_tp_plan,
        jun_tp_plan,
        jul_tp_plan,
        aug_tp_plan,
        sep_tp_plan,
        oct_tp_plan,
        nov_tp_plan,
        dec_tp_plan,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
)
select * from final