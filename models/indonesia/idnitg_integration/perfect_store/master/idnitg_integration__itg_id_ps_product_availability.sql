{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
		pre_hook = "{% if is_incremental() %}
        delete from {{this}} 
        where (outlet_id,merchandiser_id,input_date,upper(put_up_sku))
        in (select distinct trim(outlet_id),trim(merchandiser_id),cast(trim(input_date) as date),upper(trim(put_up_sku))
        from {{source ('idnsdl_raw','sdl_id_ps_product_availability')}});
        {% endif %}" 
    )
}}

with source as
(
    select * from {{source ('idnsdl_raw', 'sdl_id_ps_product_availability')}}
),
final as
(
    select 
        trim(sdl.outlet_id)::varchar(10) as outlet_id,
        trim(sdl.outlet_custom_code)::varchar(10) as outlet_custom_code,
        trim(sdl.outlet_name)::varchar(100) as outlet_name,
        trim(sdl.province)::varchar(50) as province,
        trim(sdl.city)::varchar(50) as city,
        trim(sdl.channel)::varchar(50) as channel,
        trim(sdl.merchandiser_id)::varchar(20) as merchandiser_id,
        trim(sdl.merchandiser_name)::varchar(50) as merchandiser_name,
        trim(sdl.cust_group)::varchar(50) as cust_group,
        trim(sdl.address)::varchar(255) as address,
        trim(sdl.jnj_year)::varchar(4) as jnj_year,
        trim(sdl.jnj_month)::varchar(2) as jnj_month,
        trim(sdl.jnj_week)::varchar(5) as jnj_week,
        trim(sdl.day_name)::varchar(20) as day_name,
        cast(trim(sdl.input_date) as date) as input_date,
        trim(sdl.franchise)::varchar(50) as franchise,
        trim(sdl.put_up_sku)::varchar(100) as put_up_sku,
        cast(trim(sdl.stock_qty_pcs) as integer) as stock_qty_pcs,
        trim(sdl.osa_flag)::varchar(2) as osa_flag,
        cast(trim(sdl.availability_per_franchise) as numeric(38,5)) as availability_per_franchise,
        trim(sdl.availability_osa)::varchar(30) as availability_osa,
        trim(sdl.file_name)::varchar(100) as file_name,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source sdl
)
select * from final
