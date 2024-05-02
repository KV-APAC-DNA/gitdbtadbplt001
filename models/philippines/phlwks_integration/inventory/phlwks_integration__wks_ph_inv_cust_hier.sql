with edw_vw_os_customer_dim as(
    select * from dev_dna_core.snaposeedw_integration.edw_vw_os_customer_dim
),
edw_mv_ph_customer_dim as(
    select * from dev_dna_core.snaposeedw_integration.edw_mv_ph_customer_dim
), 

transformed as(
    select distinct sap_prnt_cust_key,
			sap_prnt_cust_desc,
			parent_cust_cd,
			sap_cust_chnl_key,
			sap_cust_chnl_desc,
			sap_cust_sub_chnl_key,
			sap_sub_chnl_desc,
			sap_go_to_mdl_key,
			sap_go_to_mdl_desc,
			sap_bnr_key,
			sap_bnr_desc,
			sap_bnr_frmt_key,
			sap_bnr_frmt_desc,
			retail_env,
			t2.region_nm as region,
			t2.province_nm as zone_or_area,
			row_number() over (
				partition by sap_prnt_cust_key,
				parent_cust_cd order by sap_prnt_cust_key,
					parent_cust_cd,
					sap_cust_chnl_key,
					sap_cust_sub_chnl_key
				) as rn
		from edw_vw_os_customer_dim t1,
			edw_mv_ph_customer_dim t2
		where t1.sap_cntry_cd = 'PH'
			and ltrim(t1.sap_cust_id, '0') = ltrim(t2.cust_id(+), '0')
		
),
final as(
    select * from transformed where rn = 1
		and sap_prnt_cust_key != ''
)
select * from final