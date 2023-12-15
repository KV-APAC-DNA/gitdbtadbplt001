{{
    config(
        alias="wks_itg_crncy_exch",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        tags=[""]
    )
}}
with
    source as 
        (select * from {{ ref('aspitg_integration__stg_sdl_sap_ecc_tcurr') }}
        ),

    final as (
        select
           mandt as clnt ,
           kurst as ex_rt_typ,
           fcurr as from_crncy,
           tcurr as to_crncy,
           gdatu as vld_from,
           ukurs as ex_rt,
           ffact as from_ratio,
           tfact as to_ratio,
           current_timestamp()::timestamp_ntz(9) as crt_dttm,
           current_timestamp()::timestamp_ntz(9) as updt_dttm
           from source
        
    )

select * from final
