{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        unique_key=["bill_dt","bill_doc","initemv_week","jj_sap_dstrbtr_id","jj_sap_prod_id"],
        pre_hook="{% if is_incremental() %}
                  delete from {{this}} where (bill_dt , bill_doc , item , jj_sap_dstrbtr_id ,upper(jj_sap_prod_id)) in  (select distinct bill_dt , bill_doc , item , jj_sap_dstrbtr_id ,upper(jj_sap_prod_id) from DEV_DNA_CORE.SNAPIDNWKS_INTEGRATION.WKS_ALL_DISTRIBUTOR_SELLIN_SALES_FACT);
                  {% endif %}"
    )
}}
with source as (
    select * from  DEV_DNA_CORE.SNAPIDNWKS_INTEGRATION.WKS_ALL_DISTRIBUTOR_SELLIN_SALES_FACT
),
final as
(
    select 
        bill_dt::timestamp_ntz(9) as bill_dt,
        bill_doc::varchar(100) as bill_doc,
        item::varchar(100) as item,
        sls_doc::varchar(100) as sls_doc,
        item1::varchar(100) as item1,
        ref_doc::varchar(100) as ref_doc,
        bill_type_id::varchar(10) as bill_type_id,
        bill_type::varchar(255) as bill_type,
        jj_sap_dstrbtr_id::varchar(10) as jj_sap_dstrbtr_id,
        jj_sap_dstrbtr_nm::varchar(255) as jj_sap_dstrbtr_nm,
        jj_sap_prod_id::varchar(100) as jj_sap_prod_id,
        jj_sap_prod_desc::varchar(255) as jj_sap_prod_desc,
        numerator::number(18,4) as numerator,
        qty::number(18,4) as qty,
        uom::varchar(10) as uom,
        net_val::number(18,4) as net_val,
        curr::varchar(10) as curr,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
        filename::varchar(255) as filename
    from source
)
select * from final
