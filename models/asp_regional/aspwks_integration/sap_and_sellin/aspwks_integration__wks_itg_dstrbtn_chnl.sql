--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_dstrbtn_chnl') }}
),

--Logical CTE

--Final CTE
final as (
    select
        distr_chan,
        langu,
        txtsh,
        txtmd,
        --tgt.crt_dttm as tgt_crt_dttm,
        updt_dttm
        --case when tgt.crt_dttm is null then 'i' else 'u' end as chng_flg
    from source
)

--Final select
select * from final
