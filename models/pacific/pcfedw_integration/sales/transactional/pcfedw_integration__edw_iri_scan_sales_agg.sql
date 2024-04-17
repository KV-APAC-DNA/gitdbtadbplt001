with
edw_time_dim as
(
    select * from snappcfedw_integration.edw_time_dim
),--used a ssource
vw_iri_scan_sales_analysis as
(
    select * from snappcfedw_integration.vw_iri_scan_sales_analysis
),
iri as
(
SELECT 
    jj_year,
    jj_mnth_id,
    MAX(wk_end_dt) as wk_end_dt,
    matl_id,
    matl_desc,
    iri_ean,
    iri_market,
    representative_cust_nm,
    representative_cust_cd,
    sales_grp_cd,
    sales_grp_nm,
    SUM(scan_sales) as scan_sales
FROM vw_iri_scan_sales_analysis
WHERE UPPER(iri_market) IN (
        'AU WOOLWORTHS SCAN',
        'AU COLES GROUP SCAN',
        'AU MY CHEMIST GROUP SCAN'
    )
GROUP BY jj_year,
    jj_mnth_id,
    matl_id,
    matl_desc,
    iri_ean,
    iri_market,
    representative_cust_nm,
    representative_cust_cd,
    sales_grp_cd,
    sales_grp_nm
),
etd as 
(
select 
    jj_mnth_id,
    cal_date,
    max(cal_date) over (partition by jj_mnth_id) as max_cal_date
from edw_time_dim
group by 1,
    2
),
trans as
(
SELECT 
    iri.jj_year,
    iri.jj_mnth_id,
    etd.max_cal_date as wk_end_dt,
    iri.matl_id,
    iri.matl_desc,
    iri.iri_ean,
    iri.iri_market,
    iri.representative_cust_nm,
    iri.representative_cust_cd,
    iri.sales_grp_cd,
    iri.sales_grp_nm,
    iri.scan_sales + LAG(iri.scan_sales, 1) OVER (
        PARTITION BY iri.matl_id,
        iri.representative_cust_cd
        ORDER BY iri.matl_id,
            iri.jj_mnth_id,
            iri.representative_cust_cd
    ) + LAG(iri.scan_sales, 2) OVER (
        PARTITION BY iri.matl_id,
        iri.representative_cust_cd
        ORDER BY iri.matl_id,
            iri.jj_mnth_id,
            iri.representative_cust_cd
    ) AS scan_sales
FROM iri
    left join etd on 
    (iri.wk_end_dt::timestamp without time zone) = (etd.cal_date::date)
),
final as
(
select 
    jj_year::number(18,0) as jj_year,
	jj_mnth_id::number(18,0) as jj_mnth_id,
	wk_end_dt::date as wk_end_dt,
	matl_id::varchar(40) as matl_id,
	matl_desc::varchar(100) as matl_desc,
	iri_ean::varchar(100) as iri_ean,
	iri_market::varchar(255) as iri_market,
	representative_cust_nm::varchar(100) as representative_cust_nm,
	representative_cust_cd::varchar(100) as representative_cust_cd,
	sales_grp_cd::varchar(20) as sales_grp_cd,
	sales_grp_nm::varchar(30) as sales_grp_nm,
	scan_sales::number(38,4) as scan_sales
from trans
)
select * from final