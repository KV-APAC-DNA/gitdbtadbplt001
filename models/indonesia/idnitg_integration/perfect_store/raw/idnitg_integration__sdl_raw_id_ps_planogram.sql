with source as(
    select * from  {{ source('idnsdl_raw', 'sdl_id_ps_planogram') }}
    where file_name not in (
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_ps_planogram__null_test') }}
            union all
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_ps_planogram__duplicate_test') }}
    )
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