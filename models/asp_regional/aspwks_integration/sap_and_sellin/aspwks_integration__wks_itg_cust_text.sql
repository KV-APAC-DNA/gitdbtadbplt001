{{
    config(
        alias="wks_itg_cust_text",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        tags=["daily"]
    )
}}

with
    source as 
        (select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_ecc_customer_text') }}
        ),

    final as (
        select
            mandt,
            kunnr,
            txtmd,
            current_timestamp()::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
            from source
        
    )

select * from final
