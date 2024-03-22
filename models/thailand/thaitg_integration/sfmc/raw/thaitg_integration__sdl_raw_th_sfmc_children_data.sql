{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_th_sfmc_children_data') }}
),

final as
(
    select
        parent_key,
        child_nm,
        case 
            when trim(child_birth_mnth)::varchar ='มกราคม' then '01'::varchar
             when trim(child_birth_mnth)::varchar ='กุมภาพันธ์' then '02'::varchar
             when trim(child_birth_mnth)::varchar ='มีนาคม' then '03'::varchar
             when trim(child_birth_mnth)::varchar ='เมษายน' then '04'::varchar
			 when trim(child_birth_mnth)::varchar ='พฤษภาคม' then '05'::varchar
			 when trim(child_birth_mnth)::varchar ='มิถุนายน' then '06'::varchar
			 when trim(child_birth_mnth)::varchar ='กรกฎาคม' then '07'::varchar
			 when trim(child_birth_mnth)::varchar ='สิงหาคม' then '08'::varchar
			 when trim(child_birth_mnth)::varchar ='กันยายน' then '09'::varchar
			 when trim(child_birth_mnth)::varchar ='ตุลาคม' then '10'::varchar
			 when trim(child_birth_mnth)::varchar ='พฤศจิกายน' then '11'::varchar
			 when trim(child_birth_mnth)::varchar ='ธันวาคม' then '12'::varchar
            else 'null'::varchar
        end as child_birth_mnth,
        child_birth_year,
        child_gender,
        child_number,
        file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
{% endif %} 
)

select * from final