{{
    config(
        alias="wks_itg_company_code_text",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["bukrs"],
        merge_exclude_columns=["crt_dttm"],
        tags=[""]
    )
}}

with
    source as 
        (select * from {{ ref('stg_aspsdl_raw__sdl_sap_ecc_company_code_text') }}
        ),

    final as (
        select
          mandt,
          bukrs,
          txtmd,
          current_timestamp()::timestamp_ntz(9) as crt_dttm,
          current_timestamp()::timestamp_ntz(9) as updt_dttm
          from source
        )

select * from final
