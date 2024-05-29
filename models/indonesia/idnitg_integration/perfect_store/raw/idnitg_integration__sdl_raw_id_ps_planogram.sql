with source as(
    select * from DEV_DNA_LOAD.SNAPIDNSDL_RAW.SDL_ID_PS_PLANOGRAM
),
final as(
    select 
        trim(outlet_id) outlet_id,
        trim(outlet_custom_code) outlet_custom_code,
        trim(outlet_name) outlet_name,
        trim(province) province,
        trim(city) city,
        trim(channel) channel,
        trim(merchandiser_id) merchandiser_id,
        trim(merchandiser_name) merchandiser_name,
        trim(cust_group) cust_group,
        trim(address) address,
        trim(jnj_year) jnj_year,
        trim(jnj_month) jnj_month,
        trim(jnj_week) jnj_week,
        trim(day_name) day_name,
        trim(input_date) input_date,
        trim(franchise) franchise,
        trim(photo_link) photo_link,
        file_name as file_name,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source
    -- {% if is_incremental() %}
    -- -- this filter will only be applied on an incremental run
    -- where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    -- {% endif %}
)

select * from final