{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where cntry_cd='KR' and file_name in (select distinct file_name from {{ source('ntasdl_raw','sdl_kr_sfmc_naver_data_additional') }});
        delete from {{this}} where naver_id not in (select distinct naver_id from {% if target.name=='prod' %} ntaitg_integration.itg_kr_sfmc_naver_data {% else %} {{schema}}.ntaitg_integration__itg_kr_sfmc_naver_data {% endif %} where cntry_cd='KR' ) and cntry_cd='KR';
        {% endif %}"
)}}

with source as (
    select * from {{ source('ntasdl_raw','sdl_kr_sfmc_naver_data_additional') }} 
),
final as (
    select
        'KR'::varchar(100) as cntry_cd,
        naver_id::varchar(100) as naver_id,
        attribute_name::varchar(100) as attribute_name,
        attribute_value::varchar(100) as attribute_value,
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final