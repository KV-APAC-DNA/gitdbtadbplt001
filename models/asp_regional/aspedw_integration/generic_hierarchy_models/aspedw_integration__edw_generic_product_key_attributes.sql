with EDW_PRODUCT_KEY_ATTRIBUTES as (
     select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}

),

transformation as(
SELECT A.CTRY_NM,
                          A.EAN_UPC,
                          A.SKU,
                          A.PKA_PRODUCTKEY,
                          A.PKA_PRODUCTDESC
                   FROM (SELECT CTRY_NM,
                                LTRIM(EAN_UPC,'0') AS EAN_UPC,
                                LTRIM(MATL_NUM,'0') AS SKU,
                                PKA_PRODUCTKEY,
                                PKA_PRODUCTDESC,
                                LST_NTS AS NTS_DATE
                         FROM EDW_PRODUCT_KEY_ATTRIBUTES
                         WHERE (MATL_TYPE_CD = 'FERT' OR MATL_TYPE_CD = 'HALB' OR MATL_TYPE_CD = 'SAPR')
                         AND   LST_NTS IS NOT NULL) A
                     JOIN (SELECT CTRY_NM,
                                  LTRIM(EAN_UPC,'0') AS EAN_UPC,
                                  LTRIM(MATL_NUM,'0') AS SKU,
                                  LST_NTS AS LATEST_NTS_DATE,
                                  ROW_NUMBER() OVER (PARTITION BY CTRY_NM,EAN_UPC ORDER BY LST_NTS DESC) AS ROW_NUMBER
                           FROM EDW_PRODUCT_KEY_ATTRIBUTES
                           WHERE (MATL_TYPE_CD = 'FERT' OR MATL_TYPE_CD = 'HALB' OR MATL_TYPE_CD = 'SAPR')
                           AND   LST_NTS IS NOT NULL) B
                       ON A.CTRY_NM = B.CTRY_NM
                      AND A.EAN_UPC = B.EAN_UPC
                      AND A.SKU = B.SKU
                      AND B.LATEST_NTS_DATE = A.NTS_DATE
                      AND B.ROW_NUMBER = 1
)

--final select
select * from transformation