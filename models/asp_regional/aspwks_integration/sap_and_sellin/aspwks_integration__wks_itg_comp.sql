{{
    config(
        alias="wks_itg_comp",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["bukrs"],
        merge_exclude_columns=["crt_dttm"],
        tags=[""]
    )
}}

with
    source as (select * from {{ ref('aspsdl_raw__sdl_sap_ecc_company') }}),
    final as (
        select
            mandt,
            bukrs,
            land1,
            waers,
            ktopl,
            kkber,
            periv,
            rcomp,
            current_timestamp()  as crt_dttm,
            current_timestamp() as UPDT_DTTM
            FROM source
        
    )

select * from final
