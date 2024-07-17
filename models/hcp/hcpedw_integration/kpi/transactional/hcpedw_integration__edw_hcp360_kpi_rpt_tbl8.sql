{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where country = 'IN' AND source_system = 'INVOICE_DATA' AND activity_type = 'INVOICING' ; 
        {% endif %}"
    )
}}
with 
itg_mds_in_hcp_sales_rep_mapping as 
(
    select * from {{ ref('hcpitg_integration__itg_mds_in_hcp_sales_rep_mapping') }}
),
wks_hcp360_invoice_kpi as 
(
    select * from {{ ref('hcpwks_integration__wks_hcp360_invoice_kpi') }}
),
tempa as 
(
    with BASE AS 
(
SELECT mth_mm AS year_month,
       'IN' AS country ,
       Brand,
	    null as sales_channel,
       nvl (region_code ,'Non-Covered Area') AS region,
       nvl (zone_code ,'Non-Covered Area') AS zone,
       nvl (sales_area_code ,'Non-Covered Area') AS sales_area,
       SUM(invoice_value) AS sales_value
FROM wks_hcp360_invoice_kpi
group by 1,2,3,4,5,6,7
)
 , SALES_REP as 
(
  SELECT brand_name_code as brand,
         'IN' AS country ,
         region_code as region,
         zone_code as zone,
         hq_sales_area_code as sales_area ,
         count(*) as sales_rep_count
  FROM itg_mds_in_hcp_sales_rep_mapping
  WHERE designation IN ('MSR','HRA','EM')
  group by 1,2,3,4,5
)
SELECT 
  base.country,
  'INVOICE_DATA' AS source_system	,
  'INVOICING' AS activity_type,
  base.year_month AS year_month	,
  base.Brand AS brand	,
  base.sales_channel AS sales_channel	,
  base.region AS region	,
  base.zone AS zone	,
  base.sales_area AS sales_area	,
  base.sales_value AS sales_value	,
  SALES_REP.sales_rep_count AS sales_rep_count,
  current_timestamp() as crt_dttm	,
 current_timestamp() as updt_dttm
FROM BASE
  LEFT OUTER JOIN SALES_REP
   ON 
   base.country = SALES_REP.country
  and base.Brand = SALES_REP.Brand
  and base.region = SALES_REP.region
  and base.zone = SALES_REP.zone
  and base.sales_area = SALES_REP.sales_area
),
final as 
(
    select 
    country,
    source_system,
    activity_type,
    year_month,
    brand,
    sales_channel,
    region,
    zone,
    sales_area,
    sales_value,
    sales_rep_count,
    crt_dttm,
    updt_dttm
    from tempa
)
select * from final