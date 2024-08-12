with itg_cn_selfcare_sellout_fact as
(
    select * from chnitg_integration.itg_cn_selfcare_sellout_fact
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
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
    BASE.DSTRBTR_LVL1,
    BASE.DSTRBTR_LVL2,
    BASE.DSTRBTR_LVL3,
    BASE.ean,
    BASE.matl_num,
    BASE.Customer_Product_Desc,
    BASE.region,
    BASE.zone_or_area,
    BASE.Product_code,
    BASE.so_sls_qty,
    BASE.so_sls_value,
    BASE.msl_product_code,
    BASE.msl_product_desc,
    BASE.retail_env,
    convert_timezone('UTC',current_timestamp()) AS crtd_dttm,
    convert_timezone('UTC',current_timestamp()) AS updt_dttm
    FROM
    (
    SELECT 
        'SELL-OUT' AS DATA_SRC,
        'CN' AS CNTRY_CD,
        'China Selfcare' AS CNTRY_NM,
        b."year"::INT AS YEAR,
        b.mnth_id::INT AS MNTH_ID,
        --a.year||Right(a.week,2) AS WEEK_ID,
        TO_DATE(sellout.yearmonth||sellout.day,'YYYYMMDD') AS DAY,
        b.cal_year::INT  as univ_year,
        Right(b.cal_mnth_no,2)::INT as univ_month,
        --b.cal_year as univ_year,
        --Right(b.cal_mnth_id,2)::INT as univ_month,
        sap_sold_to as SOLDTO_CODE,
        sellercode AS DISTRIBUTOR_CODE,
        channel4 AS DISTRIBUTOR_NAME,
        buyercode AS STORE_CD,
        buyername AS STORE_NAME,
        channel2||'-'||channel3 as store_type,
        'NA' AS DSTRBTR_LVL1,
        'NA' AS DSTRBTR_LVL2,
        'NA' AS DSTRBTR_LVL3,
        'NA' AS EAN,
        sku AS MATL_NUM,
        product_name AS Customer_Product_Desc,
        region,
        CASE WHEN channel2 = 'CBD' THEN seller_province_name 
        ELSE buyer_province_name END as zone_or_area,
        product_code as Product_code,
        qty as SO_SLS_QTY,
        amount as SO_SLS_VALUE,
        product_code as msl_product_code,
        product_name as msl_product_desc,
        channel2||'-'||channel3 as retail_env
    FROM itg_cn_selfcare_sellout_fact sellout
    LEFT JOIN edw_vw_os_time_dim b 
    ON sellout.yearmonth||sellout.day = b.cal_date_id
    ) BASE
    WHERE NOT (nvl(BASE.so_sls_value, 0) = 0 and nvl(BASE.so_sls_qty, 0) = 0) AND BASE.day > (select to_date(param_value) from itg_mds_ap_customer360_config where code='min_date') 
    AND BASE.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_cn_otc')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from itg_mds_ap_customer360_config where code='base_load_cn_otc')::integer)), 'YYYYMM')
    END)
)
select * from final