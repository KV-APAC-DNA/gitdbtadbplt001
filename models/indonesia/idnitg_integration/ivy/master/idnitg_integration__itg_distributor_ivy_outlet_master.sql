{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}}
    where (upper(trim(distributorcode)), upper(trim(outletcode))) 
    in (select distinct upper(trim(distributorcode)) as distributorcode, upper(trim(outletcode)) as outletcode
    from {{source ('idnsdl_raw', 'sdl_distributor_ivy_outlet_master')}}
    where file_name not in (
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_distributor_ivy_outlet_master__null_test') }}
            union all
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_distributor_ivy_outlet_master__duplicate_test') }}
    )
    );
        {% endif %}"
    )
}}
with sdl_distributor_ivy_outlet_master as 
(
    select * from {{source ('idnsdl_raw', 'sdl_distributor_ivy_outlet_master')}}
    where file_name not in (
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_distributor_ivy_outlet_master__null_test') }}
            union all
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_distributor_ivy_outlet_master__duplicate_test') }}
    )
),
edw_distributor_customer_dim as 
(
    select * from {{ref('idnedw_integration__edw_distributor_customer_dim')}}
),
edw_channelgroup_metadata as 
(
    select * from {{ source('idnedw_integration', 'edw_channelgroup_metadata') }}
),
itg_distributor_ivy_outlet_master as (
    select 
        trim(distributorcode) as distributorcode,
        trim(usercode) as usercode,
        locationcode,
        trim(outletcode) as outletcode,
        outletname,
        outletaddress,
        trim(nvl(pincode,'')) as pincode,
        subchannelname,
        trim(classcode) as classcode,
        trim(routecode) as routecode,
        trim(visit_frequency) as visit_frequency,
        trim(visitday) as visitday,
        trim(jnj_id) as jnj_id,
        contactperson,
        credit_limit,
        invoice_limit,
        credit_period,
        trim(tin) as tin,
        trim(is_diamond_store) as is_diamond_store,
        trim(id_no) as id_no,
        trim(master_code) as master_code,
        trim(store_cluster) as store_cluster,
        trim(nvl(lattitude,'')) as lattitude,
        trim(nvl(longitude,'')) as longitude,
        cdl_dttm,
        channelname,
        tieringname,
        run_id,
        row_number() over (partition by distributorcode, outletcode order by run_id) as rn,
        file_name
    from sdl_distributor_ivy_outlet_master
),
final as (
    select 
        distributorcode::varchar(25) as distributorcode,
        usercode::varchar(25) as usercode,
        locationcode::varchar(25) as locationcode,
        outletcode::varchar(25) as outletcode,
        outletname::varchar(100) as outletname,
        outletaddress::varchar(255) as outletaddress,
        pincode::varchar(10) as pincode,
        subchannelname::varchar(100) as subchannelcode,
        classcode::varchar(15) as classcode,
        routecode::varchar(25) as routecode,
        visit_frequency::varchar(15) as visit_frequency,
        visitday::varchar(200) as visitday,
        jnj_id::varchar(20) as jnj_id,
        contactperson::varchar(255) as contactperson,
        credit_limit::number(18,2) as credit_limit,
        invoice_limit::number(18,2) as invoice_limit,
        credit_period::number(2,0) as credit_period,
        tin::varchar(50) as tin,
        is_diamond_store::varchar(3) as is_diamond_store,
        id_no::varchar(100) as id_no,
        master_code::varchar(100) as master_code,
        store_cluster::varchar(50) as store_cluster,
        lattitude::varchar(20) as lattitude,
        longitude::varchar(20) as longitude,
        cdl_dttm::varchar(200) as cdl_dttm,
        null::varchar(100) as cust_grp,
        channelname::varchar(100) as chnl,
        null::varchar(100) as chnl_grp,
        outletcode::varchar(100) as cust_id_map,
        outletname::varchar(100) as cust_nm_map, 
        null::varchar(100) as chnl_grp2,
        null::timestamp_ntz(9) as cust_crtd_dt,
        tieringname::varchar(100) as cust_grp2,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name
    from itg_distributor_ivy_outlet_master
    where rn = 1
),
updt as(
    select 
        final.distributorcode as distributorcode,
        final.usercode as usercode,
        final.locationcode as locationcode,
        final.outletcode as outletcode,
        final.outletname as outletname,
        final.outletaddress as outletaddress,
        final.pincode as pincode,
        final.subchannelcode as subchannelcode,
        final.classcode as classcode,
        final.routecode as routecode,
        final.visit_frequency as visit_frequency,
        final.visitday as visitday,
        final.jnj_id as jnj_id,
        final.contactperson as contactperson,
        final.credit_limit as credit_limit,
        final.invoice_limit as invoice_limit,
        final.credit_period as credit_period,
        final.tin as tin,
        final.is_diamond_store as is_diamond_store,
        final.id_no as id_no,
        final.master_code as master_code,
        final.store_cluster as store_cluster,
        final.lattitude as lattitude,
        final.longitude as longitude,
        final.cdl_dttm as cdl_dttm,
        nvl(edw_distributor_customer_dim.cust_grp,final.cust_grp) as cust_grp,
        final.chnl as chnl,
        nvl(edw_distributor_customer_dim.chnl_grp, final.chnl_grp) as chnl_grp,
        nvl(edw_distributor_customer_dim.cust_id_map,final.cust_id_map) as cust_id_map,
        nvl(edw_distributor_customer_dim.cust_nm_map,final.cust_nm_map) as cust_nm_map,
        nvl(edw_distributor_customer_dim.chnl_grp2, final.chnl_grp2) as chnl_grp2,
        nvl(edw_distributor_customer_dim.cust_crtd_dt, final.cust_crtd_dt) as cust_crtd_dt,
        final.cust_grp2 as cust_grp2,
        final.run_id as run_id,
        final.file_name 
    from final
    left join edw_distributor_customer_dim
    on (upper(trim(final.distributorcode)) || upper(trim(final.outletcode))) = upper(trim(edw_distributor_customer_dim.key_outlet))
),
updt2 as(
    select 
        updt.distributorcode as distributorcode,
        updt.usercode as usercode,
        updt.locationcode as locationcode,
        updt.outletcode as outletcode,
        updt.outletname as outletname,
        updt.outletaddress as outletaddress,
        updt.pincode as pincode,
        updt.subchannelcode as subchannelcode,
        updt.classcode as classcode,
        updt.routecode as routecode,
        updt.visit_frequency as visit_frequency,
        updt.visitday as visitday,
        updt.jnj_id as jnj_id,
        updt.contactperson as contactperson,
        updt.credit_limit as credit_limit,
        updt.invoice_limit as invoice_limit,
        updt.credit_period as credit_period,
        updt.tin as tin,
        updt.is_diamond_store as is_diamond_store,
        updt.id_no as id_no,
        updt.master_code as master_code,
        updt.store_cluster as store_cluster,
        updt.lattitude as lattitude,
        updt.longitude as longitude,
        updt.cdl_dttm as cdl_dttm,
        updt.cust_grp as cust_grp,
        updt.chnl as chnl,
        nvl(channelgroup.chnl_grp, updt.chnl_grp) as chnl_grp,
        updt.cust_id_map as cust_id_map,
        updt.cust_nm_map as cust_nm_map,
        updt.chnl_grp2 as chnl_grp2,
        updt.cust_crtd_dt as cust_crtd_dt,
        updt.cust_grp2 as cust_grp2,
        updt.run_id as run_id,
        updt.file_name
    from updt 
    left join edw_channelgroup_metadata channelgroup
    on updt.cust_grp2 = channelgroup.cust_grp2
)
select * from updt2