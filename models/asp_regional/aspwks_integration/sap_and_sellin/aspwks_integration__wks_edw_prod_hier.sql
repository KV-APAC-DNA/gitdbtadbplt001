
--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__itg_prod_hier') }}
),

--Logical CTE

--Final CTE
final as (
    select
        prod_hier,
        langu,
        txtmd,
        --tgt.crt_dttm as tgt_crt_dttm,
        updt_dttm
        --case when tgt.crt_dttm is null then 'i' else 'u' end as chng_flg
  from source
)

--Final select
select * from final