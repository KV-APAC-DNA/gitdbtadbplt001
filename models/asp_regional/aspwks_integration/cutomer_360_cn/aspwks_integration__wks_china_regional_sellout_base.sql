with v_rpt_jdes_cube as
(
    select * from DEV_DNA_CORE.CHNEDW_INTEGRATION.V_RPT_JDES_CUBE
),
v_rpt_pos_sales_new as
(
    select * from DEV_DNA_CORE.CHNEDW_INTEGRATION.V_RPT_POS_SALES_NEW
),
edw_cube_jdesii_jnj_cal_weekly_dim as
(
    select * from DEV_DNA_CORE.CHNEDW_INTEGRATION.EDW_CUBE_JDESII_JNJ_CAL_WEEKLY_DIM
),
edw_material_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_sales_org_dim as
(
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_company_dim as
(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
itg_mds_ap_customer360_config as
(
    select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
),
itg_mds_master_mother_code_mapping as
(
    select * from snapaspitg_integration.itg_mds_master_mother_code_mapping
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
    BASE.univ_year,
    BASE.univ_month,
    BASE.soldto_code,
    BASE.distributor_code,
    BASE.distributor_name,
    BASE.store_cd,
    BASE.store_name,
    BASE.store_type,
    BASE.DSTRBTR_LVL1,--------province
    BASE.DSTRBTR_LVL2,--------city
    BASE.DSTRBTR_LVL3,
    BASE.ean,
    BASE.matl_num,
    BASE.region,
    BASE.zone_or_area,
    BASE.Customer_Product_Desc,
    BASE.so_sls_qty,
    BASE.so_sls_value,
    nvl(mc.mother_code,'NA') as msl_product_code,
    BASE.msl_product_desc,
    BASE.retail_env,
    convert_timezone('UTC',current_timestamp()) AS crtd_dttm,
    convert_timezone('UTC',current_timestamp()) AS updt_dttm
    FROM
    (
    SELECT 
        'SELL-OUT' AS DATA_SRC,
        'CN' AS CNTRY_CD,
        'China Personal Care' AS CNTRY_NM,
        b.year::INT AS YEAR,
        --b.mnth_id AS MNTH_ID,
        b.month::INT AS MNTH_ID,
        b.year||Right(a.week,2) AS WEEK_ID,
        TO_DATE(to_char(bill_date),'YYYYMMDD') AS DAY,
        LEFT(b.date,4)::INT  as univ_year,
        SUBSTRING(b.date,5,2)::INT as univ_month,
        sold_to_code as SOLDTO_CODE,
        customer_code AS DISTRIBUTOR_CODE,
        customer_name AS DISTRIBUTOR_NAME,
        store_code AS STORE_CD,
        store_name AS STORE_NAME,
        store_type as store_type,
        province AS DSTRBTR_LVL1,
        city_by_customer AS DSTRBTR_LVL2,
        channel_name AS DSTRBTR_LVL3,
        coalesce(matsls_ean,prmry_upc_cd,'NA') as EAN,
        vsku_code AS MATL_NUM,
        region,
        area as zone_or_area,
        sku_name AS Customer_Product_Desc,
        shipped_qty_value as SO_SLS_QTY,
        trade_price_value as SO_SLS_VALUE,
        pd.msl_prod_desc as msl_product_desc,
        store_type as retail_env
    FROM v_rpt_jdes_cube a 
    left join edw_cube_jdesii_jnj_cal_weekly_dim b 
    ON a.bill_date = b.date
    left join (select matl_num,ean_num as matsls_ean
                from 
                (			
                  Select ltrim(matl_num,'0') as matl_num,ean_num,
               row_number() over (partition by ltrim(matl_num,'0') order by ean_num desc,crt_dttm desc) as rn
                    from edw_material_sales_dim where sls_org in (select distinct sls_org from edw_sales_org_dim 
                                                                        
    where sls_org_co_cd  in (select distinct co_cd from edw_company_dim where crncy_key = 'RMB' and ctry_key = 'CN' 
    and ctry_group = 'China Personal Care'))
    and (ean_num != 'N/A' and lower(ean_num) != 'na' and lower(ean_num) != 'null' and (ean_num is not null and trim(ean_num) != '') and length(ean_num) >= 12 and length(ean_num) <= 15 )) where rn = 1) matsls on LTRIM(a.vsku_code,'0') = LTRIM(matsls.matl_num,'0')
    left join (select * from (SELECT DISTINCT prmry_upc_cd,LTRIM(MATL_NUM,'0') as MATL_NUM ,row_number() over
    (PARTITION BY prmry_upc_cd order by  crt_dttm desc) as RN FROM edw_material_dim where (nullif(prmry_upc_cd,'') is not null and prmry_upc_cd != 'N/A')
    )
    where rn=1)EAN on LTRIM(a.vsku_code,'0')=LTRIM(EAN.matl_num,'0')
    left join (select distinct matl_num,case when (position(lower(pka_package_desc),'mixed') > 0 or position(lower(pka_package_desc),'assorted') > 0) then MATL_DESC
            when not(position(lower(pka_package_desc),'mixed') > 0 and position(lower(pka_package_desc),'assorted') > 0) or lower(pka_package_desc) is null then pka_product_key_description
                 end as msl_prod_desc
        from edw_material_dim)pd on LTRIM(a.vsku_code,'0')=LTRIM(pd.matl_num,'0')

    UNION ALL

    SELECT 
        'POS' AS DATA_SRC,
        'CN' AS CNTRY_CD,
        'China Personal Care' AS CNTRY_NM,
        LEFT(month,4)::INT AS YEAR,
        month::INT AS MNTH_ID,
        CASE WHEN upper(week)='ALL' or NVL(week,'NA')='NA' THEN week else (LEFT(month,4) || LPAD(Right(week,2),2,'0')) END AS WEEK_ID,
        CASE WHEN upper(week)='ALL' or NVL(week,'NA')='NA' THEN TO_DATE(month|| '01','YYYYMMDD') ELSE dateadd(week,try_to_number(LPAD(Right(week,2),2,'0')), try_to_date(LEFT(month,4), 'YYYY'))-(dayofweek(dateadd(week,try_to_number(LPAD(Right(week,2),2,'0')), try_to_date(LEFT(month,4), 'YYYY'))))::integer+1 end AS DAY,
        LEFT(month,4)::INT  as univ_year,
        Right(month,2)::INT as univ_month,
        REGEXP_replace(soldto_outlet, '[^0-9]', '') as SOLDTO_CODE,
        sold_to  AS DISTRIBUTOR_CODE,
        ka AS DISTRIBUTOR_NAME,
        store_id AS STORE_CD,
        store_out AS STORE_NAME,
        customer_grpc as store_type,
        province AS DSTRBTR_LVL1,
        city AS DSTRBTR_LVL2,
        location AS DSTRBTR_LVL3,
        coalesce(upc,matsls_ean,prmry_upc_cd,'NA') as EAN,
        p_code AS MATL_NUM,--SKU
        region_2017 as region,
        area_2017 as zone_or_area,
        item AS Customer_Product_Desc,
        pos_qty as SO_SLS_QTY,
        pos_sales as SO_SLS_VALUE ,
        pd.msl_prod_desc as msl_product_desc,
        customer_grpc as retail_env
    FROM v_rpt_pos_sales_new a 
    left join (select matl_num,ean_num as matsls_ean
                from 
                (Select ltrim(matl_num,'0') as matl_num,ean_num,
               row_number() over (partition by ltrim(matl_num,'0') order by ean_num desc,crt_dttm desc) as rn
                    from edw_material_sales_dim where sls_org in (select distinct sls_org from edw_sales_org_dim 
                                                                        
    where sls_org_co_cd  in (select distinct co_cd from edw_company_dim where crncy_key = 'RMB' and ctry_key = 'CN' 
    and ctry_group = 'China Personal Care'))
    and (ean_num != 'N/A' and lower(ean_num) != 'na' and lower(ean_num) != 'null' and (ean_num is not null and trim(ean_num) != '') and length(ean_num) >= 12 and length(ean_num) <= 15)) where rn = 1)matsls on LTRIM(a.p_code,'0') = LTRIM(matsls.matl_num,'0')
    left join (select * from (SELECT DISTINCT prmry_upc_cd,LTRIM(MATL_NUM,'0') as MATL_NUM ,row_number() over
    (PARTITION BY prmry_upc_cd order by  crt_dttm desc) as RN FROM edw_material_dim where (nullif(prmry_upc_cd,'') is not null and prmry_upc_cd != 'N/A')
    )
    where rn=1)EAN on LTRIM(a.p_code,'0')=LTRIM(EAN.matl_num,'0')
    left join (select distinct matl_num,case when (position(lower(pka_package_desc),'mixed') > 0 or position(lower(pka_package_desc),'assorted') > 0) then MATL_DESC
            when not(position(lower(pka_package_desc),'mixed') > 0 and position(lower(pka_package_desc),'assorted') > 0) or lower(pka_package_desc) is null then pka_product_key_description
                 end as msl_prod_desc
        from edw_material_dim)pd on LTRIM(a.p_code,'0')=LTRIM(pd.matl_num,'0')

    ) BASE
    left join itg_mds_master_mother_code_mapping mc on base.ean = mc.sku_unique_identifier
    WHERE NOT (nvl(BASE.so_sls_value, 0) = 0 and nvl(BASE.so_sls_qty, 0) = 0) AND BASE.day > (select to_date(param_value) from itg_mds_ap_customer360_config where code='min_date') 
    AND BASE.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_cn_skincare')='ALL' THEN '190001' ELSE to_char(add_months(to_date(convert_timezone('UTC',current_timestamp())), -((select param_value from itg_mds_ap_customer360_config where code='base_load_cn_skincare')::integer)), 'YYYYMM')
    END)
)
select * from final