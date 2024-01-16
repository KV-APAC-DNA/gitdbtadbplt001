--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_time') }}
),

--Logical CTE

--Final CTE
final as (
    select
        calday,
        fiscvarnt,
        weekday1,
        calweek,
        calmonth,
        calmonth2,
        calquart1,
        calquarter,
        halfyear1,
        calyear,
        fiscper,
        fiscper3,
        fiscyear,
        recordmode,
        --tgt.crt_dttm as tgt_crt_dttm,
        updt_dttm
        --case when tgt.crt_dttm is null then 'I' else 'U' end as chng_flg
    from source 
)

--Final select
select * from final