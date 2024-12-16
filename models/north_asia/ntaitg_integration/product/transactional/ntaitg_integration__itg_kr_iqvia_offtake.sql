with 
ITG_KR_IQVIA_OFFTAKE as (
    select * from {{ ref('ntaitg_integration__itg_kr_iqvia_offtake_history') }}
),
EDW_PRODUCT_ATTR_DIM AS (
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
ITG_MDS_KR_IQVIA_PRODUCT_MASTER AS (
    select * from {{ ref('ntaitg_integration__itg_mds_kr_iqvia_product_master') }}
),

transformed as (
    SELECT  
    a.AUDIT_CODE,
    a.DATA_PERIOD,
    a.PERIOD_YYYYMM,
    a.PRODUCT_NAME,
    a.PRODUCT_NAME_KOR,
    a.FORM_DESCRIPTION,
    a.PACK_SIZE,
    a.MOLECULE_DESC,
    a.MFR_NAME,
    a.MFR_NAME_KOR,
    a.MFR_TYPE,
    a.NHI_TYPE,
    a.CHC_1_CODE,
    a.CHC_1_DESC,
    a.CHC_2_CODE,
    a.CHC_2_DESC,
    a.CHC_3_CODE,
    a.CHC_3_DESC,
    a.CHC_4_CODE,
    a.CHC_4_DESC,
    a.ATC_1_CODE,
    a.ATC_1_DESC,
    a.ATC_2_CODE,
    a.ATC_2_DESC,
    a.ATC_3_CODE,
    a.ATC_3_DESC,
    a.ATC_4_CODE,
    a.ATC_4_DESC,
    a.UNITS,
    a.PRICE,
    a.VALUES_LC_SI_PRICE,
    a.VALUES_LC_SO_PRICE,
    -- a.FILE_NAME,
    -- a.CRTD_DTTM,
    b.EAN,
    c.PROD_HIER_L1,
    c.PROD_HIER_L2,
    c.PROD_HIER_L3,
    c.PROD_HIER_L4,
    c.PROD_HIER_L5,
    c.PROD_HIER_L6,
    c.PROD_HIER_L7,
    c.PROD_HIER_L8,
    c.PROD_HIER_L9,
FROM ITG_KR_IQVIA_OFFTAKE a

--- SDL_MDS_KR_IQVIA_PRODUCT_MASTER_ADFTEMP ---
LEFT JOIN ITG_MDS_KR_IQVIA_PRODUCT_MASTER b
    ON a.PRODUCT_NAME = b.PRODUCTNAME
    AND a.FORM_DESCRIPTION = b.FORM_DESCRIPTION
    AND a.PACK_SIZE = b.PACKSIZE::VARCHAR

---    EDW_PRODUCT_ATTR_DIM JOIN ---
LEFT JOIN EDW_PRODUCT_ATTR_DIM c
    ON b.EAN::VARCHAR = c.EAN
where B.ean is not null
),

final as (
  select * from transformed
)

select * from final