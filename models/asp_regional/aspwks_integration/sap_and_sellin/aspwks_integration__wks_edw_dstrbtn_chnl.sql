
--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__itg_dstrbtn_chnl') }}
),

--Logical CTE

--Final CTE
final as (
    select
        distr_chan,
        langu,
        txtsh,
        txtmd,
        -- null as tgt_crt_dttm,
        updt_dttm
        --null as chng_flg
  from source
)

--Final select

select * from final