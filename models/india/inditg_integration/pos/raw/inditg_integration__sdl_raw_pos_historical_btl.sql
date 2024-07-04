{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}
with sdl_pos_historical_btl as 
(
    select * from {{ source('indsdl_raw', 'sdl_pos_historical_btl') }}
),
final as
 (
    select
        mother_sku_name::varchar(255) as mother_sku_name,
	    account_name::varchar(255) as account_name,
	    re::varchar(255) as re,
	    pos_dt::date as pos_dt,
        promos::number(38,6) as promos,
	    filename::varchar(100) as filename,
	    run_id::number(14,0) as run_id,
	    crt_dttm::timestamp_ntz(9) as crt_dttm
    
    from sdl_pos_historical_btl
    {% if is_incremental() %}
    --this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
     
 )
select * from final