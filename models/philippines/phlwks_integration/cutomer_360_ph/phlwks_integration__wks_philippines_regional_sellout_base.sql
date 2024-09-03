with itg_ph_dms_sellout_sales_fact as
(
    select * from {{ ref('phlitg_integration__itg_ph_dms_sellout_sales_fact') }}
),
edw_vw_ph_dstrbtr_material_dim as
(
    select * from {{ ref('phledw_integration__edw_vw_ph_dstrbtr_material_dim') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_vw_ph_dstrbtr_customer_dim as
(
    select * from {{ ref('phledw_integration__edw_vw_ph_dstrbtr_customer_dim') }}
),
edw_mv_ph_customer_dim as
(
    select * from {{ ref('phledw_integration__edw_mv_ph_customer_dim') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_mds_ph_lav_customer as
(
    select * from {{ ref('phlitg_integration__itg_mds_ph_lav_customer') }}
),
edw_ph_pos_analysis as
(
    select * from {{ ref('phledw_integration__edw_ph_pos_analysis') }}
),
edw_ph_pos_analysis_v2 as
(
    select * from {{ ref('phledw_integration__edw_ph_pos_analysis_v2') }}
),
edw_ph_ecommerce_offtake as
(
    select * from {{ ref('phledw_integration__edw_ph_ecommerce_offtake') }}
),
itg_query_parameters as
(
    select * from {{ source('phlitg_integration', 'itg_query_parameters') }}
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
    BASE.dstrbtr_lvl1,
    BASE.dstrbtr_lvl2 ,
    BASE.dstrbtr_lvl3,
    BASE.ean,
    BASE.matl_num,
    BASE.Customer_Product_Desc,
    BASE.region,
    BASE.zone_or_area,
    BASE.so_sls_qty,
    BASE.so_sls_value,
    BASE.SO_LIST_PRICE,
    BASE.SO_SELLOUT_VALUE_LIST_PRICE,
    --BASE.msl_product_code,
    --BASE.msl_product_desc,
    BASE.channel,
    convert_timezone('UTC',current_timestamp()) AS crtd_dttm,
    convert_timezone('UTC',current_timestamp()) AS updt_dttm
    FROM
    (
    SELECT 
        'SELL-OUT' AS DATA_SRC,
        'PH' AS CNTRY_CD,
        'Philippines' AS CNTRY_NM,
        b."year"::INT AS YEAR,
        b.mnth_id::INT AS MNTH_ID,
        TO_DATE(invoice_dt) AS DAY,
        b.cal_year::INT  as univ_year,
        b.cal_mnth_no::INT as univ_month,
        'NA' AS EAN,
        nvl(mat.sap_matl_num, sls.dstrbtr_prod_id) AS MATL_NUM,
        emd.matl_desc AS Customer_Product_Desc,
        store.cust_cd AS STORE_CD,
        store.cust_nm AS STORE_NAME,
        store.outlet_type_desc as store_type,
        store.sap_soldto_code as SOLDTO_CODE,
        sls.dstrbtr_grp_cd AS DISTRIBUTOR_CODE,
        cust.PARENT_CUST_NM AS DISTRIBUTOR_NAME,
        cust.rpt_grp_2_desc AS region,
        store.sls_dstrct_cd as zone_or_area,
        store.region_nm AS DSTRBTR_LVL1,
        store.prov_nm AS DSTRBTR_LVL2,
        store.town_nm AS DSTRBTR_LVL3,
        sls.qty as SO_SLS_QTY,
        ((case when gts_val >= 0 then gts_val else 0 end) - dscnt) as SO_SLS_VALUE,
        NULL as SO_LIST_PRICE,
        NULL as SO_SELLOUT_VALUE_LIST_PRICE,
        --nvl(mat.sap_matl_num, sls.dstrbtr_prod_id) as msl_product_code,
        --emd.matl_desc as msl_product_desc,
        mds_cust.channel_desc as channel
    FROM itg_ph_dms_sellout_sales_fact sls left join
            (select distinct dstrbtr_grp_cd, dstrbtr_matl_num, sap_matl_num
                from edw_vw_ph_dstrbtr_material_dim where cntry_cd = 'PH') mat
            on sls.dstrbtr_grp_cd = mat.dstrbtr_grp_cd and upper(trim(mat.dstrbtr_matl_num)) = upper(trim(sls.dstrbtr_prod_id))
        left join edw_material_dim emd on nvl(mat.sap_matl_num, sls.dstrbtr_prod_id) = ltrim(emd.matl_num, '0')
        left join 
        (select * from edw_vw_ph_dstrbtr_customer_dim where cntry_cd = 'PH') store
        on sls.trnsfrm_cust_id = store.cust_cd
        left join edw_mv_ph_customer_dim cust ON cust.cust_id = store.sap_soldto_code
        left join edw_vw_os_time_dim b on sls.invoice_dt = b.cal_date
        left join (SELECT distinct cust_id, channel_cd, channel_desc FROM itg_mds_ph_lav_customer WHERE UPPER(active) = 'Y') mds_cust on ltrim(mds_cust.cust_id,'0') = ltrim(store.sap_soldto_code,'0')

        
    UNION ALL

    SELECT 'POS' AS DATA_SRC,
        'PH' AS 	CNTRY_CD,
        'Philippines' AS CNTRY_NM,
        left(jj_mnth_id, 4)::INT AS YEAR,
        jj_mnth_id::INT AS MNTH_ID,
        TO_DATE(jj_mnth_id|| '01','YYYYMMDD') AS DAY,
        left(jj_mnth_id, 4)::INT as univ_year,
        Right(jj_mnth_id,2)::INT as univ_month,
        'NA' AS EAN,
        sls.sku AS MATL_NUM,
        sls.mt_item_nm AS Customer_Product_Desc,
        sls.cust_brnch_cd AS STORE_CD,
        sls.mt_cust_brnch_nm AS STORE_NAME,
        'NA' as store_type,
        sls.sold_to as SOLDTO_CODE,
        sls.cust_cd AS DISTRIBUTOR_CODE,
        sls.cust_cd AS DISTRIBUTOR_NAME,
        sls.sls_grp_desc as region,
        'NA' as zone_or_area,
        sls.region_nm AS DSTRBTR_LVL1,
        sls.prov_nm AS DSTRBTR_LVL2,
        sls.mncplty_nm AS DSTRBTR_LVL3,
        jj_qty_pc as SO_SLS_QTY, 
        pos_gts as SO_SLS_VALUE,
        jj_item_prc_per_pc as SO_LIST_PRICE,
        jj_gts as SO_SELLOUT_VALUE_LIST_PRICE,
        --sku as msl_product_code,
        --mt_item_nm as msl_product_desc,
        mds_cust.channel_desc as channel
        from edw_ph_pos_analysis sls
        left join (SELECT distinct cust_id, channel_cd, channel_desc FROM itg_mds_ph_lav_customer WHERE UPPER(active) = 'Y') mds_cust on ltrim(mds_cust.cust_id,'0') = ltrim(sls.sold_to,'0')


    UNION ALL

    SELECT 'STOCK TRANSFER' AS DATA_SRC,
        'PH' AS 	CNTRY_CD,
        'Philippines' AS CNTRY_NM,
        left(jj_mnth_id, 4)::INT AS YEAR,
        jj_mnth_id::INT AS MNTH_ID,
        TO_DATE(jj_mnth_id|| '01','YYYYMMDD') AS DAY,
        left(jj_mnth_id, 4)::INT as univ_year,
        Right(jj_mnth_id,2)::INT as univ_month,
        'NA' AS EAN,
        sls_v2.sku AS MATL_NUM,
        mt_item_nm AS Customer_Product_Desc,
        cust_brnch_cd AS STORE_CD,
        mt_cust_brnch_nm AS STORE_NAME,
        'NA' as store_type,
        sold_to as SOLDTO_CODE,
        cust_cd AS DISTRIBUTOR_CODE,
        cust_cd AS DISTRIBUTOR_NAME,
        sls_grp_desc as region,
        'NA' as zone_or_area,
        region_nm AS DSTRBTR_LVL1,
        prov_nm AS DSTRBTR_LVL2,
        mncplty_nm AS DSTRBTR_LVL3,
        jj_qty_pc as SO_SLS_QTY, 
        pos_gts as SO_SLS_VALUE,
        jj_item_prc_per_pc as SO_LIST_PRICE,
        jj_gts as SO_SELLOUT_VALUE_LIST_PRICE,
        --'NA' as msl_product_code,
        --'NA' as msl_product_desc,
        mds_cust.channel_desc as channel
        from edw_ph_pos_analysis_v2 sls_v2
    left join (SELECT distinct cust_id, channel_cd, channel_desc FROM itg_mds_ph_lav_customer WHERE UPPER(active) = 'Y') mds_cust on ltrim(mds_cust.cust_id,'0') = ltrim(sls_v2.sold_to,'0')

    UNION ALL

    SELECT 'ECOM' AS DATA_SRC,
        'PH' AS 	CNTRY_CD,
        'Philippines' AS CNTRY_NM,
        b."year"::INT AS YEAR,
        b.mnth_id::INT AS MNTH_ID,
        TO_DATE(transaction_date) AS DAY,
        b.cal_year::INT as univ_year,
        b.cal_mnth_no::INT as univ_month,
        ean_upc AS EAN,
        matl_num AS MATL_NUM,
        product_desc AS Customer_Product_Desc,
        platform_name AS STORE_CD,
        platform_name AS STORE_NAME,
        'NA' as store_type,
        iqp.parameter_value as SOLDTO_CODE,
        distributor_name AS DISTRIBUTOR_CODE,
        distributor_name AS DISTRIBUTOR_NAME,
        state as region,
        'NA' as zone_or_area,
        'NA' AS DSTRBTR_LVL1,
        'NA' AS DSTRBTR_LVL2,
        'NA' AS DSTRBTR_LVL3,
        sales_quantity as SO_SLS_QTY, 
        sales_value_lcy as SO_SLS_VALUE,
        NULL as SO_LIST_PRICE,
        NULL as SO_SELLOUT_VALUE_LIST_PRICE,
        --'NA' as msl_product_code,
        --'NA' as msl_product_desc,
        'NA' as channel
        from edw_ph_ecommerce_offtake ecom left join edw_vw_os_time_dim b on ecom.cal_day = b.cal_date
        LEFT JOIN itg_query_parameters iqp
        on upper(trim(iqp.parameter_name)) = upper(trim(ecom.distributor_name)) and parameter_type='sold_to_code' and country_code='PH'
        where upper(delivery_status) <> 'CANCELLED' and upper(delivery_status) <> 'REJECTED'
    ) BASE
    WHERE NOT (nvl(BASE.so_sls_value, 0) = 0 and nvl(BASE.so_sls_qty, 0) = 0) AND BASE.day > (select to_date(param_value) from itg_mds_ap_customer360_config where code='min_date') 
    AND BASE.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_ph')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from itg_mds_ap_customer360_config where code='base_load_ph')::integer)), 'YYYYMM')
    END)
)
select * from final