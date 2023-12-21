{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}

--Import CTE
with source as (
    select *
    from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_needstates_text') }}
),

--Logical CTE

--Final CTE
final as (
    select
        zneed,
        langu,
        txtsh,
        txtmd,
        --tgt.crt_dttm as tgt_crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
        --case when tgt.crt_dttm is null then 'I' else 'U' end as chng_flg
    from source
)

--Final select
select * from final
