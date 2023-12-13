{{
    config(
        alias="wks_itg_country_code_text",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["country","langu"],
        merge_exclude_columns=["crt_dttm"],
        tags=[""]
    )
}}
with
source as 
        (select * from {{ ref('stg_aspsdl_raw__sdl_sap_bw_country_code_text') }}
        ),

    final as (
        select
            country,
            langu,
            txtsh,
            txtmd,
            current_timestamp()::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
            from source
        
    )

select * from final
