
{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook="{% if var('crm_job_to_execute') == 'th_crm_files' %}
                    delete from {{this}} where cntry_cd = 'TH';
                    {% elif var('crm_job_to_execute') == 'ph_crm_files' %}
                    delete from {{this}} where cntry_cd = 'PH'
                    {% elif var('crm_job_to_execute') == 'tw_crm_files' %}
                    delete from {{this}} where cntry_cd = 'TW';
                    {% endif %}
                "
    )
}}

with sdl_th_sfmc_children_data as
(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk 
    from {{ source('thasdl_raw', 'sdl_th_sfmc_children_data') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_children_data__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_children_data__duplicate_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_children_data__lookup_test') }}
            ) qualify rnk=1
),
itg_mds_rg_sfmc_gender as
(
    select * from {{ ref('aspitg_integration__itg_mds_rg_sfmc_gender') }}
),
sdl_ph_sfmc_children_data as
(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk from {{ source('phlsdl_raw', 'sdl_ph_sfmc_children_data') }}
),
sdl_tw_sfmc_children_data as
(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk from {{ source('ntasdl_raw', 'sdl_tw_sfmc_children_data') }}
)
{% if var("crm_job_to_execute") == 'th_crm_files' %}
,
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
    where rnk=1
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        and isc.crtd_dttm > (select max(crtd_dttm) from {{ this }})
    {% endif %}
)

select * from final

{% elif var("crm_job_to_execute") == 'ph_crm_files' %}
,
final as
(
    select
        'PH'::varchar(10) as cntry_cd,
        parent_key::varchar(200) as parent_key,
        child_nm::varchar(200) as child_nm,
        child_birth_mnth::varchar(10) as child_birth_mnth,
        child_birth_year::varchar(10) as child_birth_year,
        gen.gender_standard::varchar(10) as child_gender,
        child_number::varchar(30) as child_number,
        file_name::varchar(255) as file_name,
        isc.crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from sdl_ph_sfmc_children_data isc
        left join itg_mds_rg_sfmc_gender gen on
        upper(trim(gen.gender_raw::text)) = upper(trim(isc.child_gender::text))
    where rnk=1
)

select * from final

{% elif var("crm_job_to_execute") == 'tw_crm_files' %}
,
final as
(
    select
        'TW'::varchar(10) as cntry_cd,
        parent_key::varchar(200) as parent_key,
        child_nm::varchar(200) as child_nm,
        child_birth_mnth::varchar(10) as child_birth_mnth,
        child_birth_year::varchar(10) as child_birth_year,
        child_gender::varchar(10) as child_gender,
        child_number::varchar(30) as child_number,
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from sdl_tw_sfmc_children_data
    where rnk=1
)

select * from final

{% endif %}