{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if var('crm_job_to_execute') == 'th_crm_files' %}
        delete from {{this}} where cntry_cd='TH' 
        and crtd_dttm < (select min(crtd_dttm) 
                        from {{ source('thasdl_raw', 'sdl_th_sfmc_consumer_master_additional') }} 
                        where file_name not in (
                            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_consumer_master_additional__null_test') }}
                            union all
                            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_consumer_master_additional__duplicate_test') }}
                            union all
                            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_consumer_master_additional__lookup_test') }}
                            )
                        );
        {% elif var('crm_job_to_execute') == 'ph_crm_files' %}
        delete from {{this}} where cntry_cd='PH' 
        and crtd_dttm < (select min(crtd_dttm) 
                        from {{ source('phlsdl_raw', 'sdl_ph_sfmc_consumer_master') }}
                    );
        {% endif %}
        "
    )
}}

with source as
(
    select *, dense_rank() over(partition by null order by file_name desc) as rnk 
    from {{ source('thasdl_raw', 'sdl_th_sfmc_consumer_master_additional') }}
    where file_name not in (
                        select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_consumer_master_additional__null_test') }}
                        union all
                        select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_consumer_master_additional__duplicate_test') }}
                        union all
                        select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_consumer_master_additional__lookup_test') }}
                        ) qualify rnk=1
),
source_ph as
(
    select *,dense_rank() over(partition by null order by file_name desc) as rnk from {{ source('phlsdl_raw', 'sdl_ph_sfmc_consumer_master') }}
)
{% if var("crm_job_to_execute") == 'th_crm_files' %}
,
final as
(
    select
        'TH'::varchar(10) as cntry_cd,
        subscriber_key::varchar(100) as subscriber_key,
        attribute_name::varchar(100) as attribute_name,
        attribute_value::varchar(250) as attribute_value,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
    from source
    where rnk=1
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        and source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final

{% elif var("crm_job_to_execute") == 'ph_crm_files' %}
,
final as
(
    select
        'PH'::varchar(10) as cntry_cd,
        subscriber_key::varchar(100) as subscriber_key,
        'Viber_Engaged'::varchar(100) as attribute_name,
        Viber_Engaged::varchar(250) as attribute_value,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
    from source_ph
    where rnk=1
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
        and crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final

{% endif %}