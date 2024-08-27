with itg_sg_pos_sales_fact as (
    select * from {{ ref('sgpitg_integration__itg_sg_pos_sales_fact') }}
),
itg_mds_ap_customer360_config as (
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
    BASE.week_id,
    BASE.day,
    BASE.soldto_code,
    BASE.distributor_code,
    BASE.distributor_name,
    BASE.store_cd,
    BASE.store_name,
    BASE.store_type,
    BASE.ean,
    BASE.matl_num,
    BASE.pka_product_key,
    BASE.pka_product_key_description,
    BASE.Customer_Product_Desc,
    BASE.region,
    BASE.zone_or_area,
    BASE.master_code,
    BASE.so_sls_qty,
    BASE.so_sls_value,
    BASE.msl_product_code,
    --BASE.msl_product_desc,
    BASE.retail_env,
    convert_timezone('UTC',current_timestamp()) AS crtd_dttm,
    convert_timezone('UTC',current_timestamp()) AS updt_dttm
    FROM
    (
    --POS
    SELECT 'POS' AS DATA_SRC,
        'SG' AS 	CNTRY_CD,
        'Singapore' AS CNTRY_NM,
        year::INT AS YEAR,
        mnth_id::INT AS MNTH_ID,
        week::INT AS WEEK_ID,
        bill_date AS DAY,
        sold_to_code as SOLDTO_CODE,
        sold_to_code AS DISTRIBUTOR_CODE,
        cust_id AS DISTRIBUTOR_NAME,
        store AS STORE_CD,
        store_name AS STORE_NAME,
        store_type AS store_type,
        --'NA' AS store_type,
        product_barcode AS EAN,
        sap_code AS MATL_NUM,
        product_key AS pka_product_key,
        product_key_desc AS pka_product_key_description,
        item_desc AS Customer_Product_Desc,
        'NA' AS region,
        'NA' AS zone_or_area,
        master_code AS master_code,
        sales_qty as SO_SLS_QTY, 
        net_sales as SO_SLS_VALUE,
        master_code as msl_product_code,
            --item_desc as msl_product_desc,
            store_type as retail_env
        FROM  itg_sg_pos_sales_fact)BASE
    WHERE NOT (nvl(BASE.so_sls_value, 0) = 0 and nvl(BASE.so_sls_qty, 0) = 0) AND BASE.day > (select to_date(param_value) from itg_mds_ap_customer360_config where code='min_date') 
    AND BASE.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_sg')='ALL' THEN '190001' ELSE to_char(add_months(to_date(current_timestamp()), -((select param_value from itg_mds_ap_customer360_config where code='base_load_sg')::integer)), 'YYYYMM')
    END)
)
select * from final