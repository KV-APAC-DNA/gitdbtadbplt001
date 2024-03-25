{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['cntry_cd']
    )
}}
with 
sdl_th_sfmc_children_data as
(
    select * from {{ source('thasdl_raw', 'sdl_th_sfmc_children_data') }}
),
itg_mds_rg_sfmc_gender as 
(
    select * from {{ ref('aspitg_integration__itg_mds_rg_sfmc_gender') }}
),
final as
(
    select
        'TH'::varchar(10) as cntry_cd,
        parent_key::varchar(200) as parent_key,
        child_nm::varchar(200) as child_nm,
        child_birth_mnth::varchar(10) as child_birth_mnth,
        child_birth_year::varchar(10) as child_birth_year,
        gen.gender_standard::varchar(10) as child_gender,
        child_number::varchar(30) as child_number,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from sdl_th_sfmc_children_data isc
        left join itg_mds_rg_sfmc_gender gen on 
        upper(trim(gen.gender_raw::text)) = upper(trim(isc.child_gender::text))
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        where isc.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)

select * from final