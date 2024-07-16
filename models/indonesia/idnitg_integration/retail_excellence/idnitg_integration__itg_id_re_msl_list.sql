--import cte
with itg_re_msl_input_definition as (
    select * from  {{ source('aspitg_integration', 'itg_re_msl_input_definition') }}
),

edw_time_dim as(
    select * from {{ source('idnedw_integration', 'edw_time_dim') }}
),

wks_id_base_re as(
    select * from {{ ref('idnwks_integration__wks_id_base_re') }}
),

wks_id_c360_mapped_sku_cd as (
    select * from {{ ref('idnwks_integration__wks_id_c360_mapped_sku_cd') }}
),

edw_product_dim as (
    select * from {{ ref('idnedw_integration__edw_product_dim') }}
),

v_edw_vw_cal_retail_excellence_dim as(
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),

--final cte
itg_id_re_msl_list as (
    SELECT SUBSTRING(final.jj_mnth_id,1,4) AS jj_yr,
       final.jj_mnth_id AS jj_mnth_id,
       final.Channel AS channel_name,
       final.soldto_code AS SOLDTO_CODE,
       DISTRIBUTOR_NAME,
       DISTRIBUTOR_CODE,
       final.Channel AS Sell_Out_Channel,
       final.Sell_Out_RE AS Sell_Out_RE,
       NULL AS Prioritization_Segmentation,
       NULL AS store_category,
       final.store_code AS Store_Code,
       final.store_name AS Store_Name,
       final.store_grade AS Store_Grade,
       NULL AS Store_Size,
       final.Region AS Region,
       final.Area_Zone AS zone_name,
       final.city AS City,
       NULL AS rtrlatitude,
       NULL AS rtrlongitude,
       final.sku_unique_identifier AS put_up,
       --final.mothersku_name,
       final.market AS prod_hier_l1,
       NULL AS prod_hier_l2,
       final.franchise AS prod_hier_l3,
       final.brand AS prod_hier_l4,
       NULL AS prod_hier_l5,
       -- varient is sku_name from msl table id_itg.itg_mcs_gt
       final.put_up AS prod_hier_l6,
       NULL AS prod_hier_l7,
       NULL AS prod_hier_l8,
       NULL AS prod_hier_l9,
       final.customer_type,
       final.Channel_Type,
       final.store_type,
       final.sub_channel,
	   coalesce(nullif(LTRIM (sku_code,'0'),'NA'),ltrim(jj_sap_upgrd_prod_id,0)) as MAPPED_SKU_CD 
FROM (SELECT DISTINCT base.start_date,
             base.end_date,
             base.market,
             --base.franchise AS franchise1,
             --base.category,
             --base.brand AS brand1,
             --base.standardized_brand,
             --base.subcategory,
             base.region AS region1,
             base.zone,
             base.retail_environment,
             base.channel AS channel1,
             base.sub_channel,
             base.customer,
             base.store_grade,
             base.unique_identifier_mapping,
             base.sku_unique_identifier,
             base.jj_mnth_id,
             noo.Retail_Environment AS Sell_Out_RE,
             noo.Region,
             noo.city,
             noo.Area_Zone,
             noo.DISTRIBUTOR_NAME,
             noo.distributor_code,
             noo.customer_type,
             noo.Channel_Type, 
             noo.Channel,
             noo.store_code,
             noo.store_name,
             noo.store_type,
             noo.soldto_code,
             --noo.jj_sap_dstrbtr_nm,
             epd.franchise,
             epd.brand,
             epd.put_up,
			 sku.sku_code,
			 epd.jj_sap_upgrd_prod_id 
      FROM (SELECT *
            FROM (SELECT DISTINCT *
                  FROM itg_re_msl_input_definition
                  WHERE market = 'Indonesia') msl
              LEFT JOIN (SELECT DISTINCT jj_mnth_id,
                                jj_mnth
                         FROM edw_time_dim
						 where jj_mnth_id >= (select last_18mnths from v_edw_vw_cal_retail_excellence_dim)::numeric
						   and jj_mnth_id <= (select prev_mnth from v_edw_vw_cal_retail_excellence_dim)::numeric  
                          ) cal
                     ON TO_CHAR (TO_DATE (msl.start_date,'DD/MM/YYYY'),'YYYYMM')::NUMERIC<= cal.jj_mnth_id
                    AND TO_CHAR (TO_DATE (msl.END_DATE,'DD/MM/YYYY'),'YYYYMM')::NUMERIC>= cal.jj_mnth_id) base
          LEFT JOIN (SELECT DISTINCT RETAIL_ENVIRONMENT,Region,'NA' as city,zone_or_area as Area_Zone,DISTRIBUTOR_NAME,distributor_code,'NA' as customer_type,'NA' as Channel_Type,CHANNEL_NAME as Channel,store_code,store_name,CHANNEL_NAME as store_type, 
				   soldto_code,put_up					
                         FROM  wks_id_base_re
WHERE CNTRY_CD = 'ID' and data_src='SELL-OUT') noo										  
		                  ON UPPER (TRIM (base.sub_channel)) = UPPER (TRIM (noo.store_type)) and UPPER (TRIM (base.sku_unique_identifier)) = UPPER (TRIM (noo.put_up))
        left join (select distinct put_up,sku_code from wks_id_c360_mapped_sku_cd ) sku on UPPER(TRIM(base.sku_unique_identifier)) = UPPER(TRIM(sku.put_up))      
        LEFT JOIN (SELECT DISTINCT franchise,
                          brand,
                          variant3|| ' ' ||put_up AS put_up,
                          effective_from,
                          effective_to,jj_sap_upgrd_prod_id ,
                          ROW_NUMBER() OVER (PARTITION BY variant3|| ' ' ||put_up ORDER BY effective_from,jj_sap_upgrd_prod_id DESC) AS rno
                   FROM edw_product_dim) epd
               ON UPPER (TRIM (base.sku_unique_identifier)) = UPPER (TRIM (epd.put_up))
              AND base.jj_mnth_id BETWEEN epd.effective_from
              AND epd.effective_to
			  AND epd.rno = 1) final 
),

final as (
    select 
    jj_yr::varchar(16) as jj_yr,
    jj_mnth_id::numeric(18,0) as jj_mnth_id,
    channel_name::varchar(150) as channel_name,
    soldto_code::varchar(255) as soldto_code,
    distributor_name::varchar(356) as distributor_name,
    distributor_code::varchar(100) as distributor_code,
    sell_out_channel::varchar(150) as sell_out_channel,
    sell_out_re::varchar(150) as sell_out_re,
    prioritization_segmentation::varchar(1) as prioritization_segmentation,
    store_category::varchar(1) as store_category,
    store_code::varchar(100) as store_code,
    store_name::varchar(601) as store_name,
    store_grade::varchar(20) as store_grade,
    store_size::varchar(1) as store_size,
    region::varchar(150) as region,
    zone_name::varchar(150) as zone_name,
    city::varchar(200) as city,
    rtrlatitude::varchar(100) as rtrlatitude,
    rtrlongitude::varchar(100) as rtrlongitude,
    put_up::varchar(100) as put_up,
    prod_hier_l1::varchar(50) as prod_hier_l1,
    prod_hier_l2::varchar(100) as prod_hier_l2,
    prod_hier_l3::varchar(50) as prod_hier_l3,
    prod_hier_l4::varchar(50) as prod_hier_l4,
    prod_hier_l5::varchar(100) as prod_hier_l5,
    prod_hier_l6::varchar(60) as prod_hier_l6,
    prod_hier_l7::varchar(100) as prod_hier_l7,
    prod_hier_l8::varchar(100) as prod_hier_l8,
    prod_hier_l9::varchar(100) as prod_hier_l9,
    customer_type::varchar(200) as customer_type,
    channel_type::varchar(200) as channel_type,
    store_type::varchar(150) as store_type,
    sub_channel::varchar(50) as sub_channel,
    mapped_sku_cd::varchar(50) as mapped_sku_cd
    from itg_id_re_msl_list
)

--final select 
select * from final 
