with edw_dstrbtn_chnl as(
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
final as (
    Select distr_chan as "distr_chan",
    langu as "langu",
    txtsh as "txtsh",
    txtmd as "txtmd",
    crt_dttm as "crt_dttm",
    updt_dttm as "updt_dttm"
    From edw_dstrbtn_chnl
)

select * from final
