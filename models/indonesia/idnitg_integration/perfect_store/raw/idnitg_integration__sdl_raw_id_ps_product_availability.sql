{{
    config
    (
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as
(
    select * from {{source('idnsdl_raw', 'sdl_id_ps_product_availability')}}
),
final as
(
    select 
        trim(outlet_id)::varchar(10) as outlet_id,
        trim(outlet_custom_code)::varchar(10) as outlet_custom_code,
        trim(outlet_name)::varchar(100) as outlet_name,
        trim(province)::varchar(50) as province,
        trim(city)::varchar(50) as city,
        trim(channel)::varchar(50) as channel,
        trim(merchandiser_id)::varchar(20) as merchandiser_id,
        trim(merchandiser_name)::varchar(50) as merchandiser_name,
        trim(cust_group)::varchar(50) as cust_group,
        trim(address)::varchar(255) as address,
        trim(jnj_year)::varchar(4) as jnj_year,
        trim(jnj_month)::varchar(2) as jnj_month,
        trim(jnj_week)::varchar(5) as jnj_week,
        trim(day_name)::varchar(20) as day_name,
        trim(input_date)::varchar(20) as input_date,
        trim(franchise)::varchar(50) as franchise,
        trim(put_up_sku)::varchar(100) as put_up_sku,
        trim(stock_qty_pcs)::varchar(30) as stock_qty_pcs,
        trim(osa_flag)::varchar(2) as osa_flag,
        trim(availability_per_franchise)::varchar(30) as availability_per_franchise,
        trim(availability_osa)::varchar(30) as availability_osa,
        file_name::varchar(100) as file_name,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %})
select * from final
