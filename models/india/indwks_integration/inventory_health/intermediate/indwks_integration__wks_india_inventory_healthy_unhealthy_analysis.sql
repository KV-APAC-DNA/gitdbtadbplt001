with edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_gch_producthierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_product_dim as
( 
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
wks_india_siso_propagate_final as
(
    select * from ({{ ref('indwks_integration__wks_india_siso_propagate_final') }})
),
PRODUCT AS (
        SELECT *
        FROM (
          SELECT DISTINCT EMD.matl_num AS SAP_MATL_NUM,
            EMD.MATL_DESC AS SAP_MAT_DESC,
            EMD.MATL_TYPE_CD AS SAP_MAT_TYPE_CD,
            EMD.MATL_TYPE_DESC AS SAP_MAT_TYPE_DESC,
            --  EMD.SAP_BASE_UOM_CD AS SAP_BASE_UOM_CD,
            --  EMD.SAP_PRCHSE_UOM_CD AS SAP_PRCHSE_UOM_CD,
            EMD.PRODH1 AS SAP_PROD_SGMT_CD,
            EMD.PRODH1_TXTMD AS SAP_PROD_SGMT_DESC,
            --  EMD.SAP_BASE_PROD_CD AS SAP_BASE_PROD_CD,
            EMD.BASE_PROD_DESC AS SAP_BASE_PROD_DESC,
            --  EMD.SAP_MEGA_BRND_CD AS SAP_MEGA_BRND_CD,
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
            -- EMD.greenlight_sku_flag as greenlight_sku_flag,
            EMD.pka_product_key AS pka_product_key,
            EMD.pka_product_key_description AS pka_product_key_description,
            EMD.pka_product_key AS product_key,
            EMD.pka_size_desc AS pka_size_desc,
            EMD.pka_product_key_description AS product_key_description,
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
            row_number() OVER (
              PARTITION BY sap_matl_num ORDER BY sap_matl_num
              ) rnk
          FROM
            --(Select * from rg_edw.edw_vw_greenlight_skus where sls_org='5100')  EMD,
            (
            SELECT *
            FROM edw_material_dim
            ) EMD,
            EDW_GCH_PRODUCTHIERARCHY EGPH
          WHERE LTRIM(EMD.MATL_NUM, '0') = ltrim(EGPH.MATERIALNUMBER(+), 0)
            AND EMD.PROD_HIER_CD <> ''
            AND LTRIM(EMD.MATL_NUM, '0') IN (
              SELECT DISTINCT CAST(PRODUCT_CODE AS VARCHAR)
              FROM EDW_PRODUCT_DIM
              )
          )
        WHERE rnk = 1
        ),
trans as
(
SELECT month,
  TRIM(NVL(NULLIF(T3.GPH_PROD_BRND, ''), 'NA')) AS brand,
  TRIM(NVL(NULLIF(T3.GPH_PROD_VRNT, ''), 'NA')) AS VARIANT,
  TRIM(NVL(NULLIF(T3.GPH_PROD_SGMNT, ''), 'NA')) AS SEGMENT,
  TRIM(NVL(NULLIF(T3.GPH_PROD_CTGRY, ''), 'NA')) AS PROD_CATEGORY,
  TRIM(NVL(NULLIF(T3.pka_product_key, ''), 'NA')) AS pka_product_key,
  TRIM(NVL(NULLIF(T3.pka_size_desc, ''), 'NA')) AS pka_size_desc,
  TRIM(NVL(NULLIF(SISO.sap_parent_customer_key, ''), 'Not Assigned')) AS SAP_PRNT_CUST_KEY,
  sum(last_3months_so_value) AS last_3months_so_val,
  sum(last_6months_so_value) AS last_6months_so_val,
  sum(last_12months_so_value) AS last_12months_so_val,
  sum(last_36months_so_value) AS last_36months_so_val,
  CASE 
    WHEN COALESCE(last_36months_so_val, 0) > 0
      AND COALESCE(last_12months_so_val, 0) <= 0
      THEN 'N'
    ELSE 'Y'
    END AS healthy_inventory
FROM (
  SELECT *
  FROM wks_india_siso_propagate_final
  ) SISO,
  PRODUCT T3
WHERE LTRIM(T3.SAP_MATL_NUM(+), '0') = SISO.matl_num
GROUP BY month,
TRIM(NVL(NULLIF(T3.GPH_PROD_BRND, ''), 'NA')),
TRIM(NVL(NULLIF(T3.GPH_PROD_VRNT, ''), 'NA')),
TRIM(NVL(NULLIF(T3.GPH_PROD_SGMNT, ''), 'NA')),
TRIM(NVL(NULLIF(T3.GPH_PROD_CTGRY, ''), 'NA')),
TRIM(NVL(NULLIF(T3.pka_product_key, ''), 'NA')),
TRIM(NVL(NULLIF(T3.pka_size_desc, ''), 'NA')),
TRIM(NVL(NULLIF(SISO.sap_parent_customer_key, ''), 'Not Assigned'))
  ),
  final as
  (
    select
        month::number(38,0) as month,
	    brand::varchar(30) as brand,
	    variant::varchar(100) as variant,
	    segment::varchar(50) as segment,
	    prod_category::varchar(50) as prod_category,
	    pka_product_key::varchar(68) as pka_product_key,
	    pka_size_desc::varchar(30) as pka_size_desc,
	    sap_prnt_cust_key::varchar(50) as sap_prnt_cust_key,
	    last_3months_so_val::number(38,3) as last_3months_so_val,
	    last_6months_so_val::number(38,3) as last_6months_so_val,
	    last_12months_so_val::number(38,3) as last_12months_so_val,
	    last_36months_so_val::number(38,3) as last_36months_so_val,
	    healthy_inventory::varchar(1) as healthy_inventory
    from trans
  )
  select * from final