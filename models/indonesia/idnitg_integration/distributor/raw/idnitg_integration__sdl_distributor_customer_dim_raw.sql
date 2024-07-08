{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('idnsdl_raw', 'sdl_distributor_customer_dim') }} 
),
final as(
    select 
        key_outlet as key_outlet,
       jj_sap_dstrbtr_id as jj_sap_dstrbtr_id,
       jj_sap_dstrbtr_nm as jj_sap_dstrbtr_nm,
       cust_id as cust_id,
       cust_nm as cust_nm,
       address as address,
       city as city,
       cust_grp as cust_grp,
       chnl as chnl,
       outlet_type as outlet_type,
       chnl_grp as chnl_grp,
       jjid as jjid,
       pst_cd as pst_cd,
       cust_id_map as cust_id_map,
       cust_nm_map as cust_nm_map,
       chnl_grp2 as chnl_grp2,
       CAST(cust_crtd_dt AS TIMESTAMP) AS cust_crtd_dt,
       cust_grp2 as cust_grp2,
       CAST(crtd_dttm AS TIMESTAMP) AS crtd_dttm,
       CAST(updt_dttm AS TIMESTAMP) AS updt_dttm,
	   '200001' as effective_from,
       '999912' as effective_to, 
        NULL as filename
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)

select * from final