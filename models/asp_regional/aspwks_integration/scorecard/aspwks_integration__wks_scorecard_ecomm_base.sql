with edw_rpt_ecomm_oneview as(
    select * from snapaspedw_integration.edw_rpt_ecomm_oneview
),
edw_calendar_dim as(
    select * from snapaspedw_integration.edw_calendar_dim
),
final as(
	select 'ECOMM' as Datasource,market as ctry_nm, cluster, fisc_year,fisc_month, cast(fisc_year||'0'||lpad(fisc_month,2,'0') as integer) as fisc_per, cast(null as varchar) as parent_customer,
			sum(value_usd) ecomm_nts_usd, sum(value_lcy) ecomm_nts_lcy
	 from  edw_rpt_ecomm_oneview
	where  fisc_year >= (select fisc_yr - 4 from edw_calendar_dim where cal_day = convert_timezone('UTC', current_timestamp())::date)
	  and  kpi = 'NTS'
	group by fisc_year,fisc_month,market, cluster ,cast(fisc_year||'0'||lpad(fisc_month,2,'0') as integer) 
) 
select * from final