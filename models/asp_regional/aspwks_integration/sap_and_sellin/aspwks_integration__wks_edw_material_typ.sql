
--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__itg_material_typ') }}
),

--Logical CTE

--Final CTE
final as (
    select
        matl_type,
        langu,
        txtmd,
        --tgt.crt_dttm as tgt_crt_dttm,
        updt_dttm
        --case when tgt.crt_dttm is null then 'i' else 'u' end as chng_flg
  from source
)

--Final select
select * from final