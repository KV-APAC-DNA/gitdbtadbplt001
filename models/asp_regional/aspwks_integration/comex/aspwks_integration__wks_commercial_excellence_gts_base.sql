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
select NVL(copa.ctry_nm,'NA') :: varchar(40) AS market,
NVL(copa."cluster",'NA') :: varchar(100) AS "cluster",
NVL(cust_seg.customer_segmentation,'NA') :: varchar(500) as cust_seg,
NVL(copa."b1 mega-brand",'NA') :: varchar(100) as mega_brand,
	'GTS' :: varchar(100) AS kpi,
	(extract(year from fisc_day)||LPAD(extract(month from fisc_day),2,'00')) :: varchar(23) AS month_id,
	CASE WHEN (left(fisc_yr_per,4)||right(fisc_yr_per,2))=time_dim.mnth_id and copa.caln_day=time_dim.cal_date_id THEN time_dim.mnth_wk_no
		WHEN (left(fisc_yr_per,4)||right(fisc_yr_per,2))=m_time_dim.mnth_id and copa.caln_day>m_time_dim.max_cal_date_id THEN m_time_dim.max_mnth_wk_no
		WHEN (left(fisc_yr_per,4)||right(fisc_yr_per,2))=m_time_dim.mnth_id and copa.caln_day<m_time_dim.min_cal_date_id THEN m_time_dim.min_mnth_wk_no
		END AS week,
	gts_usd :: numeric(38,5) as gts_usd,
	gts_lcy :: numeric(38,5) as gts_lcy,
	from_crncy  :: varchar(5) as from_crncy
from v_rpt_copa_commercial_excellence copa
 left join (select ctry_nm, cust_num, customer_segmentation from vw_itg_custgp_customer_hierarchy
	where customer_segmentation is not null and customer_segmentation <> ''
	and customer_segmentation <> 'Not Available') cust_seg
on copa.ctry_nm=cust_seg.ctry_nm and LTRIM(copa.cust_num,0)= LTRIM(cust_seg.cust_num,0)
left join edw_vw_os_time_dim time_dim
  on (left(fisc_yr_per,4)||right(fisc_yr_per,2))=time_dim.mnth_id and copa.caln_day=time_dim.cal_date_id
left join
((select distinct mnth_id,max(cal_date_id) OVER (PARTITION BY mnth_id) as max_cal_date_id, max(mnth_wk_no) OVER (PARTITION BY mnth_id) as max_mnth_wk_no,
min(cal_date_id) OVER (PARTITION BY mnth_id) as min_cal_date_id, min(mnth_wk_no) OVER (PARTITION BY mnth_id) as min_mnth_wk_no from
 edw_vw_os_time_dim
	)) m_time_dim
  on (left(fisc_yr_per,4)||right(fisc_yr_per,2))=m_time_dim.mnth_id and (copa.caln_day>m_time_dim.max_cal_date_id or copa.caln_day<m_time_dim.min_cal_date_id)

)
select * from final 