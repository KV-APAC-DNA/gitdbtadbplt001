with edw_sku_recom_spike_msl as (
    select * from {{ source('indedw_integration', 'edw_sku_recom_spike_msl') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from  {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
edw_calendar_dim as 
(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_retailer_dim as
(
   select * from {{ source('indedw_integration', 'edw_retailer_dim') }}  
),
itg_udcdetails as(
  select * from {{ source('inditg_integration', 'itg_udcdetails') }}     
),

wrk_edw_sku_recom_spike_msl as 
(
SELECT substring(derived_table2.year_month,1,4) as fisc_yr,
             derived_table2.year_month AS fisc_per,
             derived_table2.channel_name as channel_name,
             derived_table2.customer_code as Distributor_Code,
             derived_table2.customer_name as Distributor_Name,
             derived_table2.business_channel as Sell_Out_Channel,
             derived_table2.retailer_category_name as Sell_Out_RE,
             derived_table2.retailer_class as Prioritization_Segmentation,
             derived_table2.program_name as store_category,
             derived_table2.urc AS Store_Code,
             upper(derived_table2.retailer_name) as Store_Name,
             derived_table2.business_channel as Store_Grade,
             derived_table2.retailer_class as Store_Size,
             derived_table2.region_name as Region,
             derived_table2.zone_name as zone_name,
             derived_table2.town_name as City,
             derived_table2.rtrlatitude,
              derived_table2.rtrlongitude,
             derived_table2.mother_sku_cd,
             derived_table2.mothersku_name,
             'India' AS prod_hier_l1,
              NULL AS prod_hier_l2,
              derived_table2.franchise_name AS prod_hier_l3,
              derived_table2.brand_name AS prod_hier_l4,
              derived_table2.variant_name AS prod_hier_l5,
              NULL AS prod_hier_l6,
              NULL AS  prod_hier_l7,
              NULL AS prod_hier_l8,
              derived_table2.mothersku_name AS prod_hier_l9,
              derived_table2.product_category_name,    
              derived_table2.msl_target,
			  sysdate() as crt_dttm
  FROM (SELECT  
				   a.year_month,
                   a.channel_name,
                   a.business_channel,
                   a.customer_code,
                   a.customer_name,
                   a.retailer_category_name,
                   CASE
                     WHEN a.channel_name::TEXT = 'Self Service Store'::CHARACTER VARYING::TEXT THEN c.columnname || ' ('::CHARACTER VARYING::TEXT || c.program_name || ')'::CHARACTER VARYING::TEXT
                     ELSE c.columnname
                   END AS program_name,
                   a.retailer_class,
                   a.urc,
                   a.retailer_name,
                   a.mother_sku_cd,
                   a.mothersku_name,
                   a.region_name,
                   a.zone_name,
                   retailer.town_name,
                   a.franchise_name,
                   a.brand_name,
                   a.product_category_name,
                   a.variant_name,
                   retailer.rtrlatitude,
                   retailer.rtrlongitude,
                   CASE
                     WHEN (a.msl_target::NUMERIC::NUMERIC(18,0)) > 0::NUMERIC::NUMERIC(18,0) THEN 1
                     ELSE NULL::INTEGER
                   END AS msl_target,
                   CASE
                     WHEN (a.msl_hit::NUMERIC::NUMERIC(18,0)) > 0::NUMERIC::NUMERIC(18,0) THEN 1
                     ELSE 0
                   END AS msl_hit,
                   CASE
                     WHEN c.distcode IS NULL THEN 'N'
                     ELSE 'Y'
                   END AS priority_store_flag
            FROM (SELECT edw_sku_recom.mth_mm AS year_month,
                         edw_sku_recom.qtr,
                          add_months(to_date(to_char(edw_sku_recom.mth_mm) ,'YYYYMM'),15),
                         max(edw_sku_recom.channel_name) as channel_name,
                         max(edw_sku_recom.business_channel) as business_channel,
                         max(edw_sku_recom.retailer_category_name) as retailer_category_name,
                         edw_sku_recom.region_name,
                         edw_sku_recom.zone_name,
                         edw_sku_recom.cust_cd AS customer_code,
                         MAX(edw_sku_recom.customer_name::TEXT) AS customer_name,
                         MAX(edw_sku_recom.class_desc) AS retailer_class,
                         MAX(edw_sku_recom.retailer_cd) AS retailer_cd,
                         edw_sku_recom.rtruniquecode AS urc,
                         MAX(edw_sku_recom.retailer_name::TEXT) AS retailer_name,
                         edw_sku_recom.franchise_name,
                         edw_sku_recom.brand_name,
                         edw_sku_recom.product_category_name,
                         edw_sku_recom.variant_name,
                         edw_sku_recom.mother_sku_cd,
                         edw_sku_recom.mothersku_name,
                         MAX(edw_sku_recom.ms_flag) AS msl_target,
                         MAX(edw_sku_recom.hit_ms_flag) AS msl_hit
                  FROM edw_sku_recom_spike_msl edw_sku_recom
                  WHERE edw_sku_recom.mothersku_name IS NOT NULL
				  and mth_mm >= (select last_17mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
						   and mth_mm <= (select prev_mnth from edw_vw_cal_Retail_excellence_Dim)::numeric  
                   -- AND   edw_sku_recom.mth_mm >(select to_char(add_months((SELECT to_date(MAX(mnth_id),'YYYYMM') FROM rg_edw.edw_rpt_regional_sellout_offtake where country_code='IN' and data_source='SELL-OUT'  ),-(select cast(parameter_value as int) from in_itg.itg_query_parameters where parameter_name='RETAIL_EXCELLENCE_TARGETS_NO_OF_MONTHS')),'yyyymm')) 
--and mth_mm <= (select to_char(sysdate,'YYYYMM'))
               
                  AND   edw_sku_recom.ms_flag::TEXT = 1::CHARACTER VARYING::TEXT
                  GROUP BY edw_sku_recom.mth_mm,
                           edw_sku_recom.qtr,
                           add_months(to_date(to_char(edw_sku_recom.mth_mm) ,'YYYYMM'),15),
                           edw_sku_recom.region_name,
                           edw_sku_recom.zone_name,
                           edw_sku_recom.cust_cd,
                           edw_sku_recom.rtruniquecode,
                           edw_sku_recom.franchise_name,
                           edw_sku_recom.brand_name,
                           edw_sku_recom.product_category_name,
                           edw_sku_recom.variant_name,
                           edw_sku_recom.mother_sku_cd,
                           edw_sku_recom.mothersku_name) a

              LEFT JOIN (SELECT derived_table1."year",
                                 derived_table1.quarter,
                                 derived_table1."month",
                                 derived_table1.distcode,
                                 derived_table1.retailer_code,
                                 MIN(derived_table1.columnname::TEXT) AS columnname,
                                 "max"(derived_table1.program_name::TEXT) AS program_name
                          FROM (SELECT t.cal_yr AS "year",
                                       t.cal_qtr_1 AS quarter,
                                       t.cal_mo_2 AS "month",
                                       u.columnname,
                                       u.columnvalue AS program_name,
                                       u.mastervaluecode AS retailer_code,
                                       u.distcode
                                FROM itg_udcdetails u
                                  JOIN edw_calendar_dim t
                                    ON "right" (u.columnname::TEXT,4) = t.cal_yr::CHARACTER VARYING::TEXT
                                   AND "left" (SPLIT_PART (u.columnname::TEXT,' Q'::CHARACTER VARYING::TEXT,2),1) = t.cal_qtr_1::CHARACTER VARYING::TEXT
                                   WHERE u.mastername::TEXT = 'Retailer Master'::CHARACTER VARYING::TEXT
                                AND   u.columnvalue IS NOT NULL
                                AND   u.columnvalue::TEXT <> 'No'::CHARACTER VARYING::TEXT
                                GROUP BY t.cal_yr,
                                         t.cal_qtr_1,
                                         t.cal_mo_2,
                                         u.columnname,
                                         u.columnvalue,
                                         u.mastervaluecode,
                                         u.distcode) derived_table1
                          GROUP BY derived_table1."year",
                                   derived_table1.quarter,
                                   derived_table1."month",
                                   derived_table1.distcode,
                                   derived_table1.retailer_code) c
                      ON a.retailer_cd::TEXT = c.retailer_code::TEXT
                     AND a.qtr = c.quarter
                     AND LTRIM ("left" (a.year_month::CHARACTER VARYING::TEXT,4),'0'::CHARACTER VARYING::TEXT) = c."year"::CHARACTER VARYING::TEXT
                     AND LTRIM ("right" (a.year_month::CHARACTER VARYING::TEXT,2),'0'::CHARACTER VARYING::TEXT) = c."month"::CHARACTER VARYING::TEXT
                     AND c.distcode::TEXT = a.customer_code::TEXT                   
          LEFT JOIN (SELECT customer_code,
                           rtruniquecode,
                           town_name,
                           rtrlatitude,
                           rtrlongitude
                      FROM (SELECT *,
                                   ROW_NUMBER() OVER (PARTITION BY customer_code,rtruniquecode ORDER BY end_date,retailer_code DESC) rn
                            FROM edw_retailer_dim)
                      WHERE rn = 1
                    ) retailer 
          ON a.customer_code = retailer.customer_code AND a.urc = retailer.rtruniquecode
) derived_table2 
),
final as 
(
select  
	fisc_yr::varchar(11) AS fisc_yr,
	fisc_per::numeric(18,0) AS fisc_per,
	channel_name::varchar(50) AS channel_name,
	distributor_code::varchar(100) AS distributor_code,
	distributor_name::varchar(200) AS distributor_name,
	sell_out_channel::varchar(50) AS sell_out_channel,
	sell_out_re::varchar(255) AS sell_out_re,
	prioritization_segmentation::varchar(50) AS prioritization_segmentation,
	store_category::varchar(203) AS store_category,
	store_code::varchar(100) AS store_code,
	store_name::varchar(382) AS store_name,
	store_grade::varchar(50) AS store_grade,
	store_size::varchar(50) AS store_size,
	region::varchar(200) AS region,
	zone_name::varchar(200) AS zone_name,
	city::varchar(50) AS city,
	rtrlatitude::varchar(40) AS rtrlatitude,
	rtrlongitude::varchar(40) AS rtrlongitude,
	mother_sku_cd::varchar(50) AS mother_sku_cd,
	mothersku_name::varchar(255) AS mothersku_name,
	prod_hier_l1::varchar(5) AS prod_hier_l1,
	prod_hier_l2::varchar(1) AS prod_hier_l2,
	prod_hier_l3::varchar(255) AS prod_hier_l3,
	prod_hier_l4::varchar(255) AS prod_hier_l4,
	prod_hier_l5::varchar(255) AS prod_hier_l5,
	prod_hier_l6::varchar(1) AS prod_hier_l6,
	prod_hier_l7::varchar(1) AS prod_hier_l7,
	prod_hier_l8::varchar(1) AS prod_hier_l8,
	prod_hier_l9::varchar(255) AS prod_hier_l9,
	product_category_name::varchar(255) AS product_category_name,
	msl_target::numeric(18,0) AS msl_target,
	crt_dttm::timestamp AS crt_dttm
from wrk_edw_sku_recom_spike_msl
)
select * from final
