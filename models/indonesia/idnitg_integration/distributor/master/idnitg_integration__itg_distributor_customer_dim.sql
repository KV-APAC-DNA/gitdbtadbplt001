with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_distributor_customer_update') }}
),
final as 
(
    select 
    trim(key_outlet)::varchar(100) as key_outlet,
	trim(sales_office_id_jnj)::varchar(50) as jj_sap_dstrbtr_id,
	trim(sales_office)::varchar(100) as jj_sap_dstrbtr_nm,
	trim(cust_id)::varchar(100) as cust_id,
	trim(cust_name)::varchar(100) as cust_nm,
	trim(address)::varchar(500) as address,
	trim(city)::varchar(100) as city,
	trim(cust_group1)::varchar(100) as cust_grp,
	trim(channel)::varchar(100) as chnl,
	trim(outlet_type)::varchar(100) as outlet_type,
	trim(channel_group1)::varchar(100) as chnl_grp,
	trim(jjid)::varchar(100) as jjid,
	trim(postal_code)::varchar(100) as pst_cd,
	trim(cust_id_map)::varchar(100) as cust_id_map,
	trim(cust_name_map)::varchar(100) as cust_nm_map,
	trim(channel_group2)::varchar(100) as chnl_grp2,
	create_date::timestamp_ntz(9) as cust_crtd_dt,
	trim(cust_group2)::varchar(100) as cust_grp2,
	convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crtd_dttm,
	convert_timezone('UTC',current_timestamp())::TIMESTAMP_NTZ(9) as UPDT_DTTM,
	nvl(effective_from,'200001')::varchar(10) as effective_from,
    nvl(effective_to,'999912')::varchar(10) as effective_to 
    from source
)
select * from final