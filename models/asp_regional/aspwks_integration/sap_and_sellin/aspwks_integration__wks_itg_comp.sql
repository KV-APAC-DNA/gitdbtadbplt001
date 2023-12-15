{{
    config(
        alias="wks_itg_comp",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        tags=[""]
    )
}}

with
    source as 
        (select * from {{ ref('aspitg_integration__stg_sdl_sap_ecc_company') }}
        ),

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
            current_timestamp()::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
            from source
        
    )

select * from final
