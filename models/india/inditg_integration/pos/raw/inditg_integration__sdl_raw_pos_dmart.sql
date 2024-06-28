{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}
with sdl_pos_dmart as 
(
    select * from {{ source('indsdl_raw', 'sdl_pos_dmart') }}
),
final as
 (
    select
        key_account_name::varchar(200) as key_account_name,
	    pos_dt::date as pos_dt,
	    store_code::varchar(50) as store_code,
	    article_code::varchar(50) as article_code,
	    subcategory::varchar(50) as subcategory,
	    level::varchar(50) as level,
	    sls_qty::number(38,6) as sls_qty,
	    sls_val_lcy::number(38,6) as sls_val_lcy,
	    filename::varchar(100) as filename,
	    run_id::number(14,0) as run_id,
	    file_upload_date::date as file_upload_date,
	    current_timestamp()::timestamp_ntz(9) as crt_dttm
    from sdl_pos_dmart
    {% if is_incremental() %}
    --this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
     
 )
select * from final