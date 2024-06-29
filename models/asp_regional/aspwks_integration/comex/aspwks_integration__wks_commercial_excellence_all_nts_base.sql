with v_rpt_copa_commercial_excellence as (
    select * from {{ ref('aspedw_integration__v_rpt_copa_commercial_excellence') }}
),
vw_itg_custgp_customer_hierarchy as ( 
    select * from {{ source('snapaspitg_integration', 'vw_itg_custgp_customer_hierarchy_sync') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
final as (
select NVL(copa.ctry_nm,'NA') :: varchar(40) AS market, NVL(copa."cluster",'NA') :: varchar(100) AS "cluster", NVL(cust_seg.customer_segmentation,'NA') :: varchar(500) as cust_seg, NVL(copa.retail_env,'NA') AS retail_env, NVL(copa."b1 mega-brand",'NA') :: varchar(100) as mega_brand,
'ALL NTS' :: varchar(100) AS kpi, extract(year from fisc_day)||LPAD(extract(month from fisc_day),2,'00') :: varchar(23) AS month_id, NVL(NULLIF(time_dim.week_passed,0),1) as ytd_week_passed, nts_usd :: numeric(38,5) as nts_usd, nts_lcy :: numeric(38,5) as nts_lcy,
 from_crncy :: varchar(5) as from_crncy
 from v_rpt_copa_commercial_excellence copa
left join (select ctry_nm, cust_num, customer_segmentation from vw_itg_custgp_customer_hierarchy 
	where customer_segmentation is not null and customer_segmentation <> ''
	and customer_segmentation <> 'Not Available') cust_seg
on copa.ctry_nm=cust_seg.ctry_nm and LTRIM(copa.cust_num,0)= LTRIM(cust_seg.cust_num,0)
left join (select mnth_id,
	case when cast(mnth_id as VARCHAR) < (extract(year from CURRENT_TIMESTAMP())||LPAD(extract(month from CURRENT_TIMESTAMP()),2,'00')) then max(wk) 
	else (select wk from edw_vw_os_time_dim 
	where cal_date= to_date(to_char(CURRENT_TIMESTAMP(), 'YYYY-MM-DD')))-1 end as week_passed
	from edw_vw_os_time_dim 
	group by 1) time_dim
on time_dim.mnth_id=(extract(year from fisc_day)||LPAD(extract(month from fisc_day),2,'00'))

)
select * from final 