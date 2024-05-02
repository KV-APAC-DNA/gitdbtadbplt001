{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('phlsdl_raw', 'sdl_ph_cpg_calls') }}
),
final as
(
    select 
      dstrbtr_grp_cd as dstrbtr_grp_cd,
       cntry_cd as cntry_cd,
       planned_visit as planned_visit,
       actual_visit as actual_visit,
       sls_rep_id as sls_rep_id,
       cust_id as cust_id,
       order_no as order_no,
       reason_no_order as reason_no_order,
       in_cpg_flag as in_cpg_flag,
       approved_flag as approved_flag,
       filename as filename,
       cdl_dttm as cdl_dttm,
       crtd_dttm as crtd_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
