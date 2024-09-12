with wks_china_siso_propagate_final as
(
    select * from {{ ref('chnwks_integration__wks_china_siso_propagate_final') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_material_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_gch_producthierarchy as 
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
product as (
  Select 
    * 
  from 
    (
      SELECT 
        DISTINCT EMD.matl_num AS SAP_MATL_NUM, 
        EMD.MATL_DESC AS SAP_MAT_DESC, 
        EMD.MATL_TYPE_CD AS SAP_MAT_TYPE_CD, 
        EMD.MATL_TYPE_DESC AS SAP_MAT_TYPE_DESC, 
        -- EMD.SAP_BASE_UOM_CD AS SAP_BASE_UOM_CD,
        -- EMD.SAP_PRCHSE_UOM_CD AS SAP_PRCHSE_UOM_CD,
        EMD.PRODH1 AS SAP_PROD_SGMT_CD, 
        EMD.PRODH1_TXTMD AS SAP_PROD_SGMT_DESC, 
        -- EMD.SAP_BASE_PROD_CD AS SAP_BASE_PROD_CD,
        EMD.BASE_PROD_DESC AS SAP_BASE_PROD_DESC, 
        --EMD.SAP_MEGA_BRND_CD AS SAP_MEGA_BRND_CD,
        EMD.MEGA_BRND_DESC AS SAP_MEGA_BRND_DESC, 
        --  EMD.SAP_BRND_CD AS SAP_BRND_CD,
        EMD.BRND_DESC AS SAP_BRND_DESC, 
        --  EMD.SAP_VRNT_CD AS SAP_VRNT_CD,
        EMD.VARNT_DESC AS SAP_VRNT_DESC, 
        --  EMD.SAP_PUT_UP_CD AS SAP_PUT_UP_CD,
        EMD.PUT_UP_DESC AS SAP_PUT_UP_DESC, 
        EMD.PRODH2 AS SAP_GRP_FRNCHSE_CD, 
        EMD.PRODH2_TXTMD AS SAP_GRP_FRNCHSE_DESC, 
        EMD.PRODH3 AS SAP_FRNCHSE_CD, 
        EMD.PRODH3_TXTMD AS SAP_FRNCHSE_DESC, 
        EMD.PRODH4 AS SAP_PROD_FRNCHSE_CD, 
        EMD.PRODH4_TXTMD AS SAP_PROD_FRNCHSE_DESC, 
        EMD.PRODH5 AS SAP_PROD_MJR_CD, 
        EMD.PRODH5_TXTMD AS SAP_PROD_MJR_DESC, 
        EMD.PRODH5 AS SAP_PROD_MNR_CD, 
        EMD.PRODH5_TXTMD AS SAP_PROD_MNR_DESC, 
        EMD.PRODH6 AS SAP_PROD_HIER_CD, 
        EMD.PRODH6_TXTMD AS SAP_PROD_HIER_DESC, 
        --EMD.greenlight_sku_flag as greenlight_sku_flag,
        EMD.pka_product_key as pka_product_key, 
        EMD.pka_size_desc as pka_size_desc, 
        EMD.pka_product_key_description as pka_product_key_description, 
        EMD.pka_product_key as product_key, 
        EMD.pka_product_key_description as product_key_description, 
        EGPH."region" AS GPH_REGION, 
        EGPH.regional_franchise AS GPH_REG_FRNCHSE, 
        EGPH.regional_franchise_group AS GPH_REG_FRNCHSE_GRP, 
        EGPH.GCPH_FRANCHISE AS GPH_PROD_FRNCHSE, 
        EGPH.GCPH_BRAND AS GPH_PROD_BRND, 
        EGPH.GCPH_SUBBRAND AS GPH_PROD_SUB_BRND, 
        EGPH.GCPH_VARIANT AS GPH_PROD_VRNT, 
        EGPH.GCPH_NEEDSTATE AS GPH_PROD_NEEDSTATE, 
        EGPH.GCPH_CATEGORY AS GPH_PROD_CTGRY, 
        EGPH.GCPH_SUBCATEGORY AS GPH_PROD_SUBCTGRY, 
        EGPH.GCPH_SEGMENT AS GPH_PROD_SGMNT, 
        EGPH.GCPH_SUBSEGMENT AS GPH_PROD_SUBSGMNT, 
        EGPH.PUT_UP_CODE AS GPH_PROD_PUT_UP_CD, 
        EGPH.PUT_UP_DESCRIPTION AS GPH_PROD_PUT_UP_DESC, 
        EGPH.SIZE AS GPH_PROD_SIZE, 
        EGPH.UNIT_OF_MEASURE AS GPH_PROD_SIZE_UOM, 
        row_number() over(
          partition by sap_matl_num 
          order by 
            sap_matl_num
        ) rnk 
      FROM 
        -- (Select * from  rg_edw.edw_vw_greenlight_skus where  SLS_ORG IN ('1500', '8888', '100A') ) EMD,
        (
          Select 
            * 
          from 
            edw_material_dim
        ) EMD, 
        EDW_GCH_PRODUCTHIERARCHY EGPH 
      WHERE 
        LTRIM(EMD.MATL_NUM, '0') = ltrim(
          EGPH.MATERIALNUMBER(+), 
          0
        ) 
        AND EMD.PROD_HIER_CD <> '' 
        AND LTRIM(EMD.MATL_NUM, '0') IN (
          SELECT 
            DISTINCT LTRIM(MATL_NUM, '0') 
          FROM 
            edw_material_sales_dim 
          WHERE 
            sls_org in ('1500', '8888', '100A')
        )
    ) 
  where 
    rnk = 1
),
final as
(
    SELECT 
    month, 
    TRIM(
        NVL(
        NULLIF(T3.GPH_PROD_BRND, ''), 
        'NA'
        )
    ) AS GLOBAL_PROD_BRAND, 
    TRIM(
        NVL(
        NULLIF(T3.GPH_PROD_VRNT, ''), 
        'NA'
        )
    ) AS GLOBAL_PROD_VARIANT, 
    TRIM(
        NVL(
        NULLIF(T3.GPH_PROD_SGMNT, ''), 
        'NA'
        )
    ) AS GLOBAL_PROD_SEGMENT, 
    TRIM(
        NVL(
        NULLIF(T3.GPH_PROD_CTGRY, ''), 
        'NA'
        )
    ) AS GLOBAL_PROD_CATEGORY, 
    TRIM(
        NVL(
        NULLIF(T3.pka_product_key, ''), 
        'NA'
        )
    ) AS pka_product_key, 
    TRIM(
        NVL(
        NULLIF(T3.pka_size_desc, ''), 
        'NA'
        )
    ) AS pka_size_desc, 
    case when SISO.SAP_PRNT_CUST_KEY = '' 
    or SISO.SAP_PRNT_CUST_KEY is null then 'Not Assigned' else TRIM(SISO.SAP_PRNT_CUST_KEY) end as SAP_PRNT_CUST_KEY, 
    sum(last_3months_so_value) as last_3months_so_val, 
    sum(last_6months_so_value) as last_6months_so_val, 
    sum(last_12months_so_value) as last_12months_so_val, 
    sum(last_36months_so_value) as last_36months_so_val, 
    CASE WHEN COALESCE(last_36months_so_val, 0)> 0 
    and COALESCE(last_12months_so_val, 0)<= 0 THEN 'N' ELSE 'Y' END AS healthy_inventory 
    FROM 
    (
        Select 
        * 
        from 
        wks_china_siso_propagate_final 
        where 
        left(month, 4)>= (
            DATE_PART(YEAR, current_timestamp()) -2
        )
    ) SISO, 
    PRODUCT T3 
    WHERE 
    LTRIM(
        T3.SAP_MATL_NUM(+), 
        '0'
    ) = ltrim(SISO.matl_num, 0) 
    GROUP BY 
    month, 
    T3.GPH_PROD_BRND, 
    T3.GPH_PROD_VRNT, 
    T3.GPH_PROD_SGMNT, 
    T3.GPH_PROD_CTGRY, 
    T3.pka_size_desc, 
    T3.pka_product_key, 
    SISO.SAP_PRNT_CUST_KEY
)
select * from final