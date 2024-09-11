with vw_iri_scan_sales_analysis as
(
    select * from {{ ref('pcfedw_integration__vw_iri_scan_sales_analysis') }}
),
edw_pacific_perenso_ims_analysis as
(
    select * from {{ ref('pcfedw_integration__edw_pacific_perenso_ims_analysis') }}
),
itg_query_parameters as
(
    select * from {{ source('pcfitg_integration', 'itg_query_parameters') }}
),
itg_mds_ap_customer360_config as
(
    select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
),
final as
(
    SELECT
    BASE.data_src,
    BASE.cntry_cd,
    BASE.cntry_nm,
    BASE.year,
    BASE.mnth_id,
    BASE.day,
    BASE.univ_year,
    BASE.univ_month,
    BASE.soldto_code,
    BASE.distributor_code,
    BASE.distributor_name,
    BASE.store_cd,
    BASE.store_name,
    BASE.store_type,
    BASE.ean,
    BASE.matl_num,
    BASE.Customer_Product_Desc,
    BASE.region,
    BASE.zone_or_area,
    BASE.distributor_additional_attribute1,
    BASE.distributor_additional_attribute2,
    BASE.distributor_additional_attribute3,
    BASE.so_sls_qty,
    BASE.so_sls_value,
    BASE.msl_product_code,
    --BASE.msl_product_desc,
    BASE.store_grade,
    UPPER(BASE.retail_env) as retail_env,
    BASE.channel,
    BASE.numeric_distribution,
    BASE.weighted_distribution,
    BASE.store_count_where_scanned,
    convert_timezone('UTC',current_timestamp()) AS crtd_dttm,
    convert_timezone('UTC',current_timestamp()) AS updt_dttm
    FROM
    (
    SELECT 'POS' AS DATA_SRC,
        CASE WHEN country = 'New Zealand' THEN 'NZ' ELSE 'AU' END AS CNTRY_CD,
        country AS CNTRY_NM,
        jj_year::INT AS YEAR,
        (jj_year || LPAD(jj_mnth,2,'00'))::INT AS MNTH_ID,
        to_date(to_char(time_id),'YYYYMMDD') AS DAY,
        cal_year::INT as univ_year,
        cal_mnth::INT as univ_month,
        representative_cust_cd as SOLDTO_CODE,
        sales_grp_cd AS DISTRIBUTOR_CODE,
        UPPER(ac_longname) AS DISTRIBUTOR_NAME,
        'NA' AS STORE_CD,
        'NA' AS STORE_NAME,
        channel_desc AS store_type,
        iri_ean AS EAN,
        NVL(matl_id,lst_sku) AS MATL_NUM,
        iri_prod_desc AS Customer_Product_Desc,
        'NA' AS region,
        'NA' AS zone_or_area,
        'NA' AS distributor_additional_attribute1,
        'NA' AS distributor_additional_attribute2,
        'NA' AS distributor_additional_attribute3,
        scan_units as SO_SLS_QTY, 
        scan_sales as SO_SLS_VALUE,
        iri_ean as msl_product_code,
        --    'NA' as msl_product_desc,
        'A' as store_grade,
        Case WHEN UPPER(ac_longname) = 'NZ WOOLWORTHS SCAN' THEN 'NZ Major Chain Super'
         WHEN UPPER(ac_longname)= 'AU MY CHEMIST GROUP SCAN' THEN 'Big Box'    
         WHEN UPPER(ac_longname) in ('NEW ZEALAND PHARMACY COMBINED (INCL CWH)','TOTAL SUBSCRIBED PHARMACY NZ') THEN 'NZ Pharmacy'
         WHEN UPPER(ac_longname) in ('AU WOOLWORTHS SCAN', 'AU COLES GROUP SCAN') THEN 'AU Major Chain Super'
         ELSE 'NA' END as retail_env,
       CASE WHEN NULLIF(TRIM(SPLIT_PART(channel_desc,'-',2)),'') IS NULL THEN TRIM(channel_desc)
         ELSE TRIM(SPLIT_PART(channel_desc,'-',2)) END AS channel,
      numeric_distribution,
      weighted_distribution,
      store_count_where_scanned
        FROM  vw_iri_scan_sales_analysis 
        WHERE NVL(representative_cust_cd,'NA')<>'NA'
        
    UNION ALL

    SELECT 'SELL-OUT' AS DATA_SRC,
        CASE WHEN order_type = 'Foodstuffs' THEN 'NZ' ELSE 'AU' END AS 	CNTRY_CD,
        CASE WHEN order_type = 'Foodstuffs' THEN 'New Zealand' ELSE 'Australia' END AS CNTRY_NM,
        jj_year::INT AS YEAR,
        (jj_year || LPAD(jj_mnth,2,'00'))::INT AS MNTH_ID,
        to_date(delvry_dt) AS DAY,
        cal_year::INT as univ_year,
        cal_mnth::INT as univ_month,
        NVL(iqp.parameter_value, 'NA') as SOLDTO_CODE,
        'NA' AS DISTRIBUTOR_CODE,
        UPPER(acct_banner) AS DISTRIBUTOR_NAME,
        acct_key::VARCHAR AS STORE_CD,
        acct_display_name AS STORE_NAME,
        acct_type_desc as store_type,
        prod_ean AS EAN,
        prod_sapbw_code AS MATL_NUM,
        prod_desc AS Customer_Product_Desc,
        acct_region AS region,
        acct_state AS zone_or_area,
        order_type AS distributor_additional_attribute1,
        'NA' AS distributor_additional_attribute2,
        'NA' AS distributor_additional_attribute3,
        unit_qty as SO_SLS_QTY, 
        nis as SO_SLS_VALUE,
        prod_ean as msl_product_code,
            --prod_desc as msl_product_desc,
            case when acct_grade in ('A','A Grade','A+ Grade','AR Grade') then 'A'
                when acct_grade in ('B','B Grade','BR Grade') then 'B'
                when acct_grade in ('C','C Grade','CR Grade') then 'C'
                when acct_grade in ('D Grade','DR Grade','E Grade','P/E Grade','G Grade','Regional','Not Assigned','Inside Sales A Grade','Inside Sales B Grade','Inside Sales C Grade','Inside Sales Other') then 'D'
                when acct_grade in ('Chemist Warehouse','Inactive Accounts') then 'Exclude'
                else 'D' end as store_grade,
            case when acct_banner in ('PSNI','PSSI') then 'Discounter'
                when acct_banner in ('NWNI','NWSI') then 'NZ Major Chain Super'
                when acct_banner in ('Chemist Warehouse','My Chemist') then 'Big Box'
                when (acct_type_desc = 'AU Pharmacy Account') and
                    (acct_banner not in ('Chemist Warehouse','My Chemist','Closed')) and
                    acct_display_name not like 'CWH%'then 'AU Indy Pharmacy'
                when acct_type_desc = 'NZ Pharmacy Account' then 'NZ Pharmacy'
                when acct_banner in ('Woolworths','Coles') then 'AU Major Chain Super' 
                when acct_type_desc in ('AU Independent Grocery Account') then 'Independent Grocery' end  as retail_env,
            acct_banner_division as channel,
            0 AS numeric_distribution,
            0 AS weighted_distribution,
            0 AS store_count_where_scanned
        FROM edw_pacific_perenso_ims_analysis sls
        LEFT JOIN itg_query_parameters iqp
        on upper(trim(iqp.parameter_name)) = upper(trim(sls.order_type)) and iqp.parameter_type='order_type' and iqp.country_code='Pacific'   
        WHERE order_type in ('Shipped Weekly', 'Metcash Weekly', 'Foodstuffs')
        )BASE
    WHERE NOT (nvl(BASE.so_sls_value, 0) = 0 and nvl(BASE.so_sls_qty, 0) = 0) AND BASE.day > (select to_date(param_value) from itg_mds_ap_customer360_config where code='min_date') 
    AND BASE.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_anz')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from itg_mds_ap_customer360_config where code='base_load_anz')::integer)), 'YYYYMM')
    END)
)
select * from final