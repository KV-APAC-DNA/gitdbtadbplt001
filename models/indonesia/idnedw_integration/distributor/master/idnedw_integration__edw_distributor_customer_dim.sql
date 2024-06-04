with source as (
 select * from  {{ref('idnitg_integration__itg_distributor_customer_dim')}}
 
),
final as (
    SELECT 
        key_outlet::VARCHAR(100) as key_outlet,
        trim(jj_sap_dstrbtr_id)::VARCHAR(50) as jj_sap_dstrbtr_id,
        jj_sap_dstrbtr_nm::VARCHAR(100) as jj_sap_dstrbtr_nm,
        trim(cust_id)::VARCHAR(100) as cust_id,
        cust_nm::VARCHAR(100) as cust_nm,
        address::VARCHAR(500) as address,
        city::VARCHAR(100) as city,
        cust_grp::VARCHAR(100) as cust_grp,
        chnl::VARCHAR(100) as chnl,
        outlet_type::VARCHAR(100) as outlet_type,
        chnl_grp::VARCHAR(100) as chnl_grp,
        case 
            when (trim(jjid) = '' or jjid is null or upper(trim(jjid)) = 'NULL' or jjid = '0') 
                then (trim(jj_sap_dstrbtr_id::varchar) ||trim (cust_id::varchar)) 
            else           	
                trim(jjid::varchar) 
        end::VARCHAR(100) as jjid,
        pst_cd::varchar(100) as pst_cd,
        cust_id_map::varchar(100) as cust_id_map,
        cust_nm_map::varchar(100) as cust_nm_map,
        chnl_grp2::varchar(100) as chnl_grp2,
        cust_crtd_dt::timestamp_ntz(9) as cust_crtd_dt,
        cust_grp2::varchar(100) as cust_grp2,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) AS updt_dttm,
        effective_from::VARCHAR(10) as effective_from,
        effective_to::VARCHAR(10) as effective_to,
        null::VARCHAR(255) as filename
    from source 
    where effective_to = '999912'
)
select * from final