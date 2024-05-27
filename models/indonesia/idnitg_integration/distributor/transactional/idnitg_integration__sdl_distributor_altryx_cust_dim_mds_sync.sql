with source as (
    select * from  DEV_DNA_CORE.SNAPIDNEDW_INTEGRATION.EDW_DISTRIBUTOR_CUSTOMER_DIM_TEMP
),
final as
(
    select 
        key_outlet::varchar(100) as key_outlet,
        jj_sap_dstrbtr_id::varchar(50) as jj_sap_dstrbtr_id,
        jj_sap_dstrbtr_nm::varchar(100) as jj_sap_dstrbtr_nm,
        cust_id::varchar(100) as cust_id,
        cust_nm::varchar(100) as cust_nm,
        address::varchar(500) as address,
        city::varchar(100) as city,
        cust_grp::varchar(100) as cust_grp,
        chnl::varchar(100) as chnl,
        outlet_type::varchar(100) as outlet_type,
        chnl_grp::varchar(100) as chnl_grp,
        jjid::varchar(100) as jjid,
        pst_cd::varchar(100) as pst_cd,
        cust_id_map::varchar(100) as cust_id_map,
        cust_nm_map::varchar(100) as cust_nm_map,
        chnl_grp2::varchar(100) as chnl_grp2,
        cust_crtd_dt::timestamp_ntz(9) as cust_crtd_dt,
        cust_grp2::varchar(100) as cust_grp2,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        '200001'::varchar(10) as effective_from,
        '999912'::varchar(10) as effective_to
    from source
)
select * from final
