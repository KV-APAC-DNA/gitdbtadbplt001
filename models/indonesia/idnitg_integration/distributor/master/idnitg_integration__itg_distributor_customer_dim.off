with source as
(
    select * from {{ ref('idnwks_integration__wks_distributor_customer_dim_temp') }}
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
	convert_timezone('utc', current_timestamp())::timestamp_ntz(9) as crtd_dttm,
	convert_timezone('utc', current_timestamp())::timestamp_ntz(9) as updt_dttm,
	effective_from::varchar(10) as effective_from,
	effective_to::varchar(10) as effective_to,
    from source
)
select * from final