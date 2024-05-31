{{
    config
        (
        materialized='table',
        post_hook=["UPDATE {{this}} SET CUST_GRP = edcd.CUST_GRP, CHNL_GRP = edcd.CHNL_GRP, CHNL_GRP2 = edcd.CHNL_GRP2, CUST_CRTD_DT = edcd.CUST_CRTD_DT FROM dev_dna_core.idnedw_integration.EDW_DISTRIBUTOR_CUSTOMER_DIM edcd WHERE  upper(trim({{this}}.KEY_OUTLET)) =  upper(trim(edcd.KEY_OUTLET));", "update {{this}} outletmaster set chnl_grp=channelgroup.chnl_grp from dev_dna_core.idnedw_integration.edw_channelgroup_metadata channelgroup where outletmaster.cust_grp2=channelgroup.cust_grp2;"]
        )
}}

with itg_distributor_ivy_outlet_master as 
(
    select * from {{ref('idnitg_integration__itg_distributor_ivy_outlet_master')}}
),
edw_distributor_dim as
(
    select * from {{ref('idnedw_integration__edw_distributor_dim')}}
),
edw_distributor_customer_dim as 
(
    select * from {{ref('idnedw_integration__edw_distributor_customer_dim')}}
),
edw_channelgroup_metadata as (
    select * from {{ref('idnedw_integration__edw_channelgroup_metadata')}}
),
transformed as 
(
    select
        (trim(idiom.distributorcode) || trim(idiom.outletcode))::varchar(100) as key_outlet,
        trim(idiom.distributorcode)::varchar(50) as jj_sap_dstrbtr_id,
        jj_sap_dstrbtr_nm::varchar(100) as jj_sap_dstrbtr_nm,
        trim(idiom.outletcode)::varchar(100) as cust_id,
        idiom.outletname::varchar(100) as cust_nm,
        idiom.outletaddress::varchar(500) as address,
        idiom.locationcode::varchar(100) as city,
        null::varchar(100) as cust_grp,
        chnl::varchar(100) as chnl,
        idiom.subchannelcode::varchar(100) as outlet_type,
        null::varchar(100) as chnl_grp,
        idiom.jnj_id::varchar(100) as jjid,
        idiom.pincode::varchar(100) as pst_cd,
        trim(idiom.outletcode)::varchar(100) as cust_id_map,
        idiom.outletname::varchar(100) as cust_nm_map,
        null::varchar(100) as chnl_grp2,
        null::timestamp_ntz(9) as cust_crtd_dt,
        cust_grp2::varchar(100) as cust_grp2,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
    from itg_distributor_ivy_outlet_master idiom
    join
    (select jj_sap_dstrbtr_id,jj_sap_dstrbtr_nm,row_number() over (partition by jj_sap_dstrbtr_id,jj_sap_dstrbtr_nm order by effective_to desc) as rnk
    from edw_distributor_dim) edm
    on  upper(trim(idiom.distributorcode)) = upper(trim(edm.jj_sap_dstrbtr_id))
    where edm.rnk=1
)
select * from transformed
-- updt as(
--     select 
--         transformed.key_outlet as key_outlet,
--         transformed.jj_sap_dstrbtr_id as jj_sap_dstrbtr_id,
--         transformed.jj_sap_dstrbtr_nm as jj_sap_dstrbtr_nm,
--         transformed.cust_id as cust_id,
--         transformed.cust_nm as cust_nm,
--         transformed.address as address,
--         transformed.city as city,
--         nvl(edw_distributor_customer_dim.cust_grp,transformed.cust_grp) as cust_grp,
--         transformed.chnl as chnl,
--         transformed.outlet_type as outlet_type,
--         nvl(edw_distributor_customer_dim.chnl_grp, transformed.chnl_grp) as chnl_grp,
--         transformed.jjid as jjid,
--         transformed.pst_cd as pst_cd,
--         transformed.cust_id_map as cust_id_map,
--         transformed.cust_nm_map as cust_nm_map,
--         nvl(edw_distributor_customer_dim.chnl_grp2, transformed.chnl_grp2) as chnl_grp2,
--         nvl(edw_distributor_customer_dim.cust_crtd_dt, transformed.cust_crtd_dt) as cust_crtd_dt,
--         transformed.cust_grp2 as cust_grp2,
--         transformed.crtd_dttm as crtd_dttm,
--         transformed.updt_dttm as updt_dttm
--     from transformed
--     left join edw_distributor_customer_dim
--     on upper(trim(transformed.key_outlet)) = upper(trim(edw_distributor_customer_dim.key_outlet))
-- ),
-- updt2 as(
--   select 
--     	updt.key_outlet as key_outlet,
--         updt.jj_sap_dstrbtr_id as jj_sap_dstrbtr_id,
--         updt.jj_sap_dstrbtr_nm as jj_sap_dstrbtr_nm,
--         updt.cust_id as cust_id,
--         updt.cust_nm as cust_nm,
--         updt.address as address,
--         updt.city as city,
--         updt.cust_grp as cust_grp,
--         updt.chnl as chnl,
--         updt.outlet_type as outlet_type,
--         nvl(channelgroup.chnl_grp, updt.chnl_grp) as chnl_grp,
--         updt.jjid as jjid,
--         updt.pst_cd as pst_cd,
--         updt.cust_id_map as cust_id_map,
--         updt.cust_nm_map as cust_nm_map,
--         updt.chnl_grp2 as chnl_grp2,
--         updt.cust_crtd_dt as cust_crtd_dt,
--         updt.cust_grp2 as cust_grp2,
--         updt.crtd_dttm as crtd_dttm,
--         updt.updt_dttm as updt_dttm
--     from updt 
--     left join edw_channelgroup_metadata channelgroup
--     on updt.cust_grp2=channelgroup.cust_grp2
-- )

