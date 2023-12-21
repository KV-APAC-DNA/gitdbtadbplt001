{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}


--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__itg_code_descriptions') }}
),

--Logical CTE

--Final CTE
final as (
    select
        source_type,
        code_type,
        code,
        code_desc,
        --tgt.crt_dttm as tgt_crt_dttm,
        updt_dttm
        --case when tgt.crt_dttm is null then 'i' else 'u' end as chng_flg
  from source
)

--Final select
select * from final
