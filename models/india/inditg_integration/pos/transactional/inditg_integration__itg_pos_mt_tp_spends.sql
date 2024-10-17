{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
                        
                        delete from {{this}} WHERE (UPPER(TRIM(account_name)),UPPER(TRIM(year)),UPPER(TRIM(mthmm)),UPPER(TRIM(article_code))) 
                        IN (SELECT DISTINCT UPPER(TRIM(account_name)),UPPER(TRIM(year)),UPPER(TRIM(mthmm)),UPPER(TRIM(article_code))
                        FROM {{ source('indsdl_raw', 'sdl_mt_tp_spends') }});
                    
                {% endif %}"
    )
}}
with sdl_mt_tp_spends as
(
    select * from {{ source('indsdl_raw', 'sdl_mt_tp_spends') }}
),

transformed as
(
    select
        upper(account_name)::varchar(255) as account_name,
        year::varchar(20) as year,
        mthmm::varchar(20) as mthmm,
        article_code::varchar(255) as article_code,
        article_desc::varchar(255) as article_desc,
        claim_amnt::number(38,6) as claim_amnt,
        sap_code_jntl::varchar(100) as sap_code_jntl,
        motherskuname::varchar(255) as motherskuname,
        volume::number(38,6) as volume,
        franchise::varchar(255) as franchise,
        remark::varchar(255) as remark,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        filename::varchar(255) as source_file_name,
        file_upload_date::date as file_upload_date,
        run_id::number(14,0) as run_id
    from sdl_mt_tp_spends
)

select * from transformed
