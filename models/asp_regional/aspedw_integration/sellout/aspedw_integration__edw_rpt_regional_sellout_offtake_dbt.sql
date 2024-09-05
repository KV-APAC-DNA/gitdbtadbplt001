{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} WHERE country_name='China Personal Care' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_cn_skincare')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_cn_skincare')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE COUNTRY_CODE='JP' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_jp')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_jp')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE COUNTRY_CODE='IN' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_in')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_in')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE COUNTRY_CODE='SG' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_sg')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_sg')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE country_code='TH' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_th')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_th')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE country_code='MY' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_my')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_my')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE country_code='KR' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_kr')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_kr')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE country_name='China Selfcare' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_cn_otc')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_cn_otc')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE country_code='ID' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_id')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_id')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE country_code='PH' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_ph')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_ph')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE country_code='HK' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_hk')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_hk')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE (country_code='NZ' OR country_code='AU') AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_anz')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_anz')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE country_code='TW' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_tw')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_tw')::integer)), 'YYYYMM') END);
        delete from {{this}} WHERE country_code='VN' AND mnth_id>= (case when (select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_vn')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }} where code='base_load_vn')::integer)), 'YYYYMM') END);
        {% endif %}"
    )
}}

with itg_mds_ap_customer360_config as
(
    select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
),
jp_wks_rpt_regional_sellout_offtake_npd as
(
    select * from {{ ref('jpnwks_integration__wks_rpt_regional_sellout_offtake_npd') }}
),
in_wks_rpt_regional_sellout_offtake_npd as
(
    select * from {{ ref('indwks_integration__wks_rpt_regional_sellout_offtake_npd') }}
),
sdl_raw_sap_bw_price_list as
(
    select * from {{ ref('aspitg_integration__sdl_raw_sap_bw_price_list') }}
),
edw_sales_org_dim as
(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_list_price as
(
    select * from {{ ref('aspedw_integration__edw_list_price') }}
),
wks_singapore_regional_sellout_offtake_npd as
(
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_offtake_npd') }}
),
edw_vw_greenlight_skus as
(
    select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
th_wks_rpt_regional_sellout_offtake_npd as
(
    select * from {{ ref('thawks_integration__wks_rpt_regional_sellout_offtake_npd') }}
),
wks_rpt_regional_my_sellout_offtake_npd as
(
    select * from {{ ref('myswks_integration__wks_rpt_regional_my_sellout_offtake_npd') }}
),
wks_korea_regional_sellout_offtake_npd as
(
    select * from {{ ref('ntawks_integration__wks_korea_regional_sellout_offtake_npd') }}
),
wks_rpt_regional_id_sellout_offtake_npd as
(
    select * from {{ ref('idnwks_integration__wks_rpt_regional_id_sellout_offtake_npd') }}
),
wks_hong_kong_regional_sellout_offtake_npd as
(
    select * from {{ ref('ntawks_integration__wks_hong_kong_regional_sellout_offtake_npd') }}
),
wks_taiwan_regional_sellout_offtake_npd as 
(
    select * from {{ ref('ntawks_integration__wks_taiwan_regional_sellout_offtake_npd') }}
),
wks_vietnam_regional_sellout_offtake_npd as
(
    select * from {{ ref('vnwks_integration__wks_vietnam_regional_sellout_offtake_npd') }}
),
cn_wks_rpt_regional_sellout_offtake_npd as
(
    select * from {{ ref('chnwks_integration__wks_rpt_regional_sellout_offtake_npd') }}
),
wks_rpt_regional_cn_sc_sellout_offtake_npd as
(
    select * from {{ ref('chnwks_integration__wks_rpt_regional_cn_sc_sellout_offtake_npd') }}
),
wks_rpt_regional_ph_sellout_offtake_npd as
(
    select * from {{ ref('phlwks_integration__wks_rpt_regional_ph_sellout_offtake_npd') }}
),
wks_rpt_regional_pacific_sellout_offtake_npd as
(
    select * from {{ ref('pcfwks_integration__wks_rpt_regional_pacific_sellout_offtake_npd') }}
),
final as 
(
    --------------------------- China Personal Care ----------------------------
    SELECT * FROM (SELECT main.year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek(main.cal_date) = 0 THEN -6-dayofweek(main.cal_date) ELSE 1-dayofweek(main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        main.univ_year,
        main.univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL ,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        'NA' AS store_grade,
        main.retail_env,
        'NA' AS channel,
        main.crtd_dttm,
        main.updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned
        FROM cn_wks_rpt_regional_sellout_offtake_npd main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'CN')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )	   
        where year > (select max(year) from cn_wks_rpt_regional_sellout_offtake_npd)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer

    UNION ALL

    --------------------------- Japan ----------------------------
    SELECT * FROM (SELECT main.year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek(main.cal_date) = 0 THEN -6-dayofweek(main.cal_date) ELSE 1-dayofweek(main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        date_part(year,cal_date) AS univ_year,
        date_part(month,cal_date) AS univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL, 
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        'NA' AS store_grade,
        main.retail_env,
        main.channel,
        main.crtd_dttm,
        main.updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned
        FROM jp_wks_rpt_regional_sellout_offtake_npd main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list where sls_org IN 
        (select distinct sls_org from edw_sales_org_dim where stats_crncy = 'JPY')
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in 
          (select distinct sls_org from edw_sales_org_dim where stats_crncy = 'JPY')
        ) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )	   
        where year > (select max(year) from jp_wks_rpt_regional_sellout_offtake_npd)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer

    UNION ALL

    --------------------------- India ----------------------------

    SELECT * FROM (SELECT main.year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek(main.cal_date) = 0 THEN -6-dayofweek(main.cal_date) ELSE 1-dayofweek(main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        main.univ_year,
        main.univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        main.store_grade,
        main.retail_env,
        main.channel,
        main.crtd_dttm,
        main.updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned
        FROM in_wks_rpt_regional_sellout_offtake_npd main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list where sls_org IN 
        (select distinct sls_org from edw_sales_org_dim where stats_crncy IN ('INR', 'LKR', 'BDT'))
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'IN')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )	   
        where year > (select max(year) from in_wks_rpt_regional_sellout_offtake_npd)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer
    
    UNION ALL

    --------------------------- Singapore ----------------------------

    SELECT * FROM (SELECT main.year::integer as year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no::integer,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek( main.cal_date) = 0 THEN -6-dayofweek( main.cal_date) ELSE 1-dayofweek( main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        date_part(year,cal_date) AS univ_year,
        date_part(month,cal_date) AS univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL ,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        'NA' AS store_grade,
        main.retail_env,
        'NA' AS channel,
        main.crtd_dttm,
        main.updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned
        FROM WKS_SINGAPORE_REGIONAL_SELLOUT_OFFTAKE_NPD main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list 
        where sls_org IN (SELECT distinct sls_org FROM edw_vw_greenlight_skus WHERE ctry_group='Singapore' and ctry_nm ='Singapore')
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (SELECT distinct sls_org FROM edw_vw_greenlight_skus WHERE ctry_group='Singapore' and ctry_nm ='Singapore')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )              
        where year > (select max(year) from wks_singapore_regional_sellout_offtake_npd)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer
    
    UNION ALL

    --------------------------- Thailand ----------------------------

    SELECT * FROM (SELECT main.year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek( main.cal_date) = 0 THEN -6-dayofweek( main.cal_date) ELSE 1-dayofweek( main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        date_part(year,cal_date) AS univ_year,
        date_part(month,cal_date) AS univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currecy,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL ,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        'NA' AS store_grade,
        main.retail_env,
        main.channel,
        main.crtd_dttm,
        main.updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned
        FROM th_wks_rpt_regional_sellout_offtake_npd main
        left join (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key in ('TH'))) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )	   
        where year > (select max(year) from th_wks_rpt_regional_sellout_offtake_npd)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer

    UNION ALL

    --------------------------- Malaysia ----------------------------

    SELECT * FROM (SELECT main.year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek( main.cal_date) = 0 THEN -6-dayofweek( main.cal_date) ELSE 1-dayofweek( main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        main.univ_year,
        main.univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL ,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        main.store_grade,
        main.retail_env,
        'NA' AS channel,
        main.crtd_dttm,
        main.updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned
        FROM wks_rpt_regional_my_sellout_offtake_npd main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'MY')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )	   
        where year > (select max(year) from wks_rpt_regional_my_sellout_offtake_npd)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer

    UNION ALL

    --------------------------- Korea ----------------------------

    SELECT * FROM (SELECT main.year::integer as year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no::integer,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek( main.cal_date) = 0 THEN -6-dayofweek( main.cal_date) ELSE 1-dayofweek( main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        date_part(year,cal_date) AS univ_year,
        date_part(month,cal_date) AS univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        CASE WHEN main.data_source::text = 'SELL-OUT' THEN main.soldto_code
             ELSE main.distributor_code
        END AS distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL ,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        'NA' AS store_grade,
        main.retail_env,
        main.channel,
        main.crtd_dttm,
        main.updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned
        FROM WKS_KOREA_REGIONAL_SELLOUT_OFFTAKE_NPD main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list 
        where sls_org IN (select distinct sls_org from edw_sales_org_dim where stats_crncy='KRW')
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'KR')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )              
        where year > (select max(year) from WKS_KOREA_REGIONAL_SELLOUT_OFFTAKE_NPD)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer

    UNION ALL

    --------------------------- China Selfcare ----------------------------

    SELECT * FROM (SELECT main.year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek( main.cal_date) = 0 THEN -6-dayofweek( main.cal_date) ELSE 1-dayofweek( main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        main.univ_year,
        main.univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        'NA' AS store_grade,
        main.retail_env,
        'NA' AS channel,
        main.crtd_dttm,
        main.updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned 
        FROM wks_rpt_regional_cn_sc_sellout_offtake_npd main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'CN')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )	   
        where year > (select max(year) from wks_rpt_regional_cn_sc_sellout_offtake_npd)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer

    UNION ALL

    --------------------------- Indonesia ----------------------------

    SELECT * FROM (SELECT main.year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek( main.cal_date) = 0 THEN -6-dayofweek( main.cal_date) ELSE 1-dayofweek( main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        main.univ_year,
        main.univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL ,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        'NA' AS store_grade,
        main.retail_env,
        main.channel,
        main.crtd_dttm,
        main.updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned	
        FROM wks_rpt_regional_id_sellout_offtake_npd main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'ID')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )	   
        where year > (select max(year) from wks_rpt_regional_id_sellout_offtake_npd)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer

    UNION ALL

    --------------------------- Philippines ----------------------------

    SELECT * FROM (SELECT main.year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek( main.cal_date) = 0 THEN -6-dayofweek( main.cal_date) ELSE 1-dayofweek( main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        main.univ_year,
        main.univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        CASE WHEN main.data_source IN ('POS','STOCK TRANSFER') THEN main.list_price::numeric(38,6) ELSE NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) END AS list_price,
        CASE WHEN main.data_source IN ('POS','STOCK TRANSFER') THEN main.sellout_value_list_price::numeric(38,6) ELSE (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity END AS sellout_value_list_price,
        CASE WHEN main.data_source IN ('POS','STOCK TRANSFER') THEN main.sellout_value_list_price::numeric(38,6)*main.exchange_rate ELSE ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate END AS sellout_value_list_price_usd ,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        'NA' AS store_grade,
        'NA' AS retail_env,
        main.channel,
        main.crtd_dttm,
        main.updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned 
        FROM wks_rpt_regional_ph_sellout_offtake_npd main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'PH')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )	   
        where year > (select max(year) from wks_rpt_regional_ph_sellout_offtake_npd)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer

    UNION ALL

    --------------------------- Hong Kong ----------------------------

    SELECT * FROM (SELECT main.year::integer as year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no::integer,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek( main.cal_date) = 0 THEN -6-dayofweek( main.cal_date) ELSE 1-dayofweek( main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        main.univ_year,
        main.univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL ,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        main.store_grade,
        main.retail_env,
        main.channel,
        main.crtd_dttm,
        main.updt_dttm,
	    0 AS numeric_distribution, 
	    0 AS weighted_distribution,
	    0 AS store_count_where_scanned
        FROM WKS_HONG_KONG_REGIONAL_SELLOUT_OFFTAKE_NPD main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC, b.ctry_key NULLS LAST) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list 
        where sls_org IN (select distinct sls_org from edw_sales_org_dim where stats_crncy='HKD')
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'HK')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )              
        where year > (select max(year) from WKS_HONG_KONG_REGIONAL_SELLOUT_OFFTAKE_NPD)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer

    UNION ALL

    --------------------------- Pacific ----------------------------

    SELECT * FROM (SELECT main.year::integer as year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no::integer,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek( main.cal_date) = 0 THEN -6-dayofweek( main.cal_date) ELSE 1-dayofweek( main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        main.univ_year,
        main.univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL ,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        main.store_grade,
        main.retail_env,
        main.channel,
        main.crtd_dttm,
        main.updt_dttm,
        main.numeric_distribution,
        main.weighted_distribution,
        main.store_count_where_scanned
        FROM wks_rpt_regional_pacific_sellout_offtake_npd main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list 
        where sls_org IN (select distinct sls_org from edw_sales_org_dim where stats_crncy='AUD' or stats_crncy='NZD')
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'AU' or ctry_key = 'NZ')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )              
        where year > (select max(year) from wks_rpt_regional_pacific_sellout_offtake_npd)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer

    UNION ALL

    --------------------------- Taiwan ----------------------------

    SELECT * FROM (SELECT main.year::integer as year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no::integer,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek( main.cal_date) = 0 THEN -6-dayofweek( main.cal_date) ELSE 1-dayofweek( main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        main.univ_year,
        main.univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL ,
        main.Customer_Product_Desc,
        main.msl_product_code,
        main.msl_product_desc,
        'NA' as store_grade,
        main.retail_env,
        main.channel,
        main.crtd_dttm,
        main.updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned
        FROM WKS_TAIWAN_REGIONAL_SELLOUT_OFFTAKE_NPD main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list 
        where sls_org IN (select distinct sls_org from edw_sales_org_dim where stats_crncy='TWD')
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'TW')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )              
        where year > (select max(year) from WKS_TAIWAN_REGIONAL_SELLOUT_OFFTAKE_NPD)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer

    UNION ALL

    --------------------------- Vietnam ----------------------------

    SELECT * FROM (SELECT main.year::integer as year,
        main.qrtr_no,
        main.mnth_id,
        main.mnth_no::integer,
        main.cal_date,
        to_date(dateadd(d, (CASE WHEN dayofweek( main.cal_date) = 0 THEN -6-dayofweek( main.cal_date) ELSE 1-dayofweek( main.cal_date) END), main.cal_date)) AS WEEK_START_DATE,
        main.univ_year,
        main.univ_month,
        main.country_code,
        main.country_name,
        main.data_source,
        main.soldto_code,
        main.distributor_code,
        main.distributor_name,
        main.store_code,
        main.store_name,
        main.store_type,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
        main.DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
        main.sap_parent_customer_key,
        main.sap_parent_customer_description,
        main.sap_customer_channel_key,
        main.sap_customer_channel_description,
        main.sap_customer_sub_channel_key,
        main.sap_sub_channel_description,
        main.sap_go_to_mdl_key,
        main.sap_go_to_mdl_description,
        main.sap_banner_key,
        main.sap_banner_description,
        main.sap_banner_format_key,
        main.sap_banner_format_description,
        main.retail_environment,
        main.region,
        main.zone_or_area,
        main.customer_segment_key,
        main.customer_segment_description,
        main.global_product_franchise,
        main.global_product_brand,
        main.global_product_sub_brand,
        main.global_product_variant,
        main.global_product_segment,
        main.global_product_subsegment,
        main.global_product_category,
        main.global_product_subcategory,
        main.global_put_up_description,
        main.ean,
        ltrim(main.sku_code,'0') as sku_code,
        main.sku_description,
        main.pka_product_key,
        main.pka_product_key_description,
        main.from_currency,
        main.to_currency,
        main.exchange_rate,
        main.sellout_sales_quantity,
        main.sellout_sales_value,
        main.sellout_sales_value_usd,
        NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)) AS list_price,
        (NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity AS sellout_value_list_price,
        ((NVL(lp1.amount::numeric(38,6),lp2.amount::numeric(38,6)))*main.sellout_sales_quantity)*main.exchange_rate AS sellout_value_list_price_usd,
        main.selling_price,
        FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
        FIRST_SCAN_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL='Y' AND cnt_mkt>0 AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Market_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_MARKET_LEVEL,
        CASE WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL='Y' AND cal_date<=dateadd(week,(select param_value from itg_mds_ap_customer360_config where code='npd_flag_period_weeks')::integer,Customer_Product_Min_Date) THEN 'Y' ELSE 'N' END AS NPD_FLAG_PARENT_CUSTOMER_LEVEL ,
        main.Customer_Product_Desc,
        ltrim(main.msl_product_code,'0') as msl_product_code,
        main.msl_product_desc,
        'NA' AS store_grade,
        main.retail_env,
        main.channel,
        main.crtd_dttm,
        main.updt_dttm,
        0 AS numeric_distribution,
        0 AS weighted_distribution,
        0 AS store_count_where_scanned
        FROM WKS_VIETNAM_REGIONAL_SELLOUT_OFFTAKE_NPD main
        left join (Select * from (select distinct a.material,a.dt_from,a.valid_to,a.amount,a.sls_org,b.ctry_key,row_number() OVER(PARTITION BY 
        ltrim(a.material, 0) ORDER BY to_date(a.valid_to, 'YYYYMMDD') DESC, to_date(a.dt_from, 'YYYYMMDD') DESC) AS rn FROM 
        (select material,dt_from,valid_to,max(amount) as amount,sls_org,cdl_dttm,currency from sdl_raw_sap_bw_price_list 
        where sls_org IN (select distinct sls_org from edw_sales_org_dim where stats_crncy='VND')
        group by material,dt_from,valid_to,sls_org,cdl_dttm,currency) a LEFT JOIN (select distinct ctry_key,sls_org,crncy_key from edw_sales_org_dim where ctry_key<>'') b on a.sls_org=b.sls_org and a.currency=b.crncy_key and a.cdl_dttm = (select max(cdl_dttm) 
        from sdl_raw_sap_bw_price_list)) where rn=1) lp1 on ltrim(main.sku_code,'0') = ltrim(lp1.material, '0') and 
        TRIM(UPPER(main.country_code))=TRIM(UPPER(lp1.ctry_key)) and (main.cal_date between to_date(dt_from, 'YYYYMMDD') and 
        to_date(valid_to, 'YYYYMMDD'))   
        left join (Select * from (SELECT distinct ltrim(material, 0) AS material, amount, row_number() OVER(PARTITION BY 
        ltrim(material, 0) ORDER BY to_date(valid_to, 'YYYYMMDD') DESC, to_date(dt_from, 'YYYYMMDD') DESC) AS rn FROM edw_list_price 
        WHERE sls_org in (select distinct sls_org from edw_sales_org_dim where ctry_key = 'VN')) where rn = 1) lp2 on ltrim(main.sku_code, '0') = ltrim(lp2.material, '0')
        )              
        where year > (select max(year) from WKS_VIETNAM_REGIONAL_SELLOUT_OFFTAKE_NPD)::integer - (select param_value from itg_mds_ap_customer360_config where code='retention_years')::integer
)
select year::number(18,0) as year,
    qrtr_no::varchar(14) as qrtr_no,
    mnth_id::varchar(23) as mnth_id,
    mnth_no::number(18,0) as mnth_no,
    cal_date::date as cal_date,
    week_start_date::date as week_start_date,
    univ_year::number(18,0) as univ_year,
    univ_month::number(18,0) as univ_month,
    country_code::varchar(2) as country_code,
    country_name::varchar(50) as country_name,
    data_source::varchar(14) as data_source,
    soldto_code::varchar(255) as soldto_code,
    distributor_code::varchar(100) as distributor_code,
    distributor_name::varchar(255) as distributor_name,
    store_code::varchar(100) as store_code,
    store_name::varchar(500) as store_name,
    store_type::varchar(255) as store_type,
    distributor_additional_attribute1::varchar(150) as distributor_additional_attribute1,
    distributor_additional_attribute2::varchar(150) as distributor_additional_attribute2,
    distributor_additional_attribute3::varchar(150) as distributor_additional_attribute3,
    sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
    sap_parent_customer_description::varchar(75) as sap_parent_customer_description,
    sap_customer_channel_key::varchar(12) as sap_customer_channel_key,
    sap_customer_channel_description::varchar(75) as sap_customer_channel_description,
    sap_customer_sub_channel_key::varchar(12) as sap_customer_sub_channel_key,
    sap_sub_channel_description::varchar(75) as sap_sub_channel_description,
    sap_go_to_mdl_key::varchar(12) as sap_go_to_mdl_key,
    sap_go_to_mdl_description::varchar(75) as sap_go_to_mdl_description,
    sap_banner_key::varchar(12) as sap_banner_key,
    sap_banner_description::varchar(75) as sap_banner_description,
    sap_banner_format_key::varchar(12) as sap_banner_format_key,
    sap_banner_format_description::varchar(75) as sap_banner_format_description,
    retail_environment::varchar(50) as retail_environment,
    region::varchar(150) as region,
    zone_or_area::varchar(150) as zone_or_area,
    customer_segment_key::varchar(12) as customer_segment_key,
    customer_segment_description::varchar(50) as customer_segment_description,
    global_product_franchise::varchar(30) as global_product_franchise,
    global_product_brand::varchar(30) as global_product_brand,
    global_product_sub_brand::varchar(100) as global_product_sub_brand,
    global_product_variant::varchar(100) as global_product_variant,
    global_product_segment::varchar(50) as global_product_segment,
    global_product_subsegment::varchar(100) as global_product_subsegment,
    global_product_category::varchar(50) as global_product_category,
    global_product_subcategory::varchar(50) as global_product_subcategory,
    global_put_up_description::varchar(100) as global_put_up_description,
    ean::varchar(50) as ean,
    sku_code::varchar(40) as sku_code,
    sku_description::varchar(150) as sku_description,
    pka_product_key::varchar(68) as pka_product_key,
    pka_product_key_description::varchar(255) as pka_product_key_description,
    from_currency::varchar(5) as from_currency,
    to_currency::varchar(5) as to_currency,
    exchange_rate::number(15,5) as exchange_rate,
    sellout_sales_quantity::number(38,6) as sellout_sales_quantity,
    sellout_sales_value::number(38,6) as sellout_sales_value,
    sellout_sales_value_usd::number(38,11) as sellout_sales_value_usd,
    list_price::number(38,6) as list_price,
    sellout_value_list_price::number(38,12) as sellout_value_list_price,
    sellout_value_list_price_usd::number(38,17) as sellout_value_list_price_usd,
    selling_price::number(38,4) as selling_price,
    first_scan_flag_parent_customer_level::varchar(1) as first_scan_flag_parent_customer_level,
    first_scan_flag_market_level::varchar(1) as first_scan_flag_market_level,
    npd_flag_market_level::varchar(1) as npd_flag_market_level,
    npd_flag_parent_customer_level::varchar(1) as npd_flag_parent_customer_level,
    customer_product_desc::varchar(300) as customer_product_desc,
    msl_product_code::varchar(150) as msl_product_code,
    msl_product_desc::varchar(300) as msl_product_desc,
    store_grade::varchar(150) as store_grade,
    retail_env::varchar(150) as retail_env,
    channel::varchar(150) as channel,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    numeric_distribution::number(20,4) as numeric_distribution,
    weighted_distribution::number(20,4) as weighted_distribution,
    store_count_where_scanned::number(20,4) as store_count_where_scanned
 from final