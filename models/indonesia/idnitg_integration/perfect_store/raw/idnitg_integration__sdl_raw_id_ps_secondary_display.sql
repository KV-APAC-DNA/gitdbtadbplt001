with source as(
    select * from {{ source('idnsdl_raw', 'sdl_id_ps_secondary_display') }}
),
final as(
    select 
        trim(outlet_id) as outlet_id,
        trim(outlet_custom_code) as outlet_custom_code,
        trim(outlet_name) as outlet_name,
        trim(province) as province,
        trim(city) as city,
        trim(channel) as channel,
        trim(merchandiser_id) as merchandiser_id,
        trim(merchandiser_name) as merchandiser_name,
        trim(cust_group) as cust_group,
        trim(address) as address,
        trim(jnj_year) as jnj_year,
        trim(jnj_month) as jnj_month,
        trim(jnj_week) as jnj_week,
        trim(day_name) as day_name,
        trim(input_date) as input_date,
        trim(franchise) as franchise,
        trim(photo_link) as photo_link,
        trim(rent) as rent,
        file_name as file_name,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final