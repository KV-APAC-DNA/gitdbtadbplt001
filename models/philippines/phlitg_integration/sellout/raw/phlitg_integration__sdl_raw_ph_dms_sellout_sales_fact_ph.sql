{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('phlsdl_raw', 'sdl_ph_dms_sellout_sales_fact') }}
),
final as
(
    select 
        dstrbtr_grp_cd as dstrbtr_grp_cd,
        cntry_cd as cntry_cd,
        dstrbtr_cust_id as dstrbtr_cust_id,
        order_dt as order_dt,
        invoice_dt as invoice_dt,
        order_no as order_no,
        invoice_no as invoice_no,
        sls_route_id as sls_route_id,
        sls_route_nm as sls_route_nm,
        sls_grp as sls_grp,
        sls_rep_id as sls_rep_id,
        sls_rep_nm as sls_rep_nm,
        dstrbtr_prod_id as dstrbtr_prod_id,
        uom as uom,
        gross_price as gross_price,
        qty as qty,
        gts_val as gts_val,
        dscnt as dscnt,
        nts_val as nts_val,
        line_num as line_num,
        prom_id as prom_id,
        wh_id as wh_id,
        cdl_dttm as cdl_dttm,
        curr_date as curr_date,
        null as filename,
        sls_rep_type as sls_rep_type,
        order_qty as order_qty,
        order_delivery_dt as order_delivery_dt,
        order_status as order_status
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.curr_date > (select max(curr_date) from {{ this }}) 
    {% endif %}
)
select * from final
