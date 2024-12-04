with apc_a902 AS
(
  SELECT *
  FROM {{ ref ('aspitg_integration__vw_stg_sdl_sap_apc_a902') }}
),
apc_konp AS
(
  SELECT *
  FROM {{ ref ('aspitg_integration__vw_stg_sdl_sap_apc_konp') }}
),
apc_mvke AS
(
  SELECT *
  FROM {{ ref ('aspitg_integration__vw_stg_sdl_sap_apc_mvke') }}
),
EDW_SALES_ORG_DIM AS 
(
    SELECT *
    FROM {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
EDW_MATERIAL_UOM AS (
    SELECT * FROM {{ ref('aspedw_integration__edw_material_uom') }}
),

EDW_MATERIAL_DIM AS (
    SELECT * FROM {{ ref('aspedw_integration__edw_material_dim') }}
    WHERE MATL_TYPE_CD IN ('FERT','HALB','SAPR')
),

material_number_union AS (
select distinct matnr as matl_num, vkorg from apc_mvke
UNION
select distinct PMATN as matl_num, vkorg from apc_mvke
UNION
select distinct matnr as matl_num, vkorg from apc_a902
UNION 
select distinct matl_num as matl_num, null as vkorg from edw_material_dim
),
material_join AS (
    SELECT A.matl_num,
    a.org,
    b.matnr,
    CASE 
    WHEN b.VTWEG = 19 AND b.VKORG IN ('3300','3410') THEN NULL
    ELSE NULLIF(b.PMATN,'') 
    END AS pmatn,
    -- NULLIF(b.PMATN,'') as pmatn,
    b.vkorg,
    b.vtweg
    FROM (SELECT DISTINCT MATL_NUM,vkorg as org 
    FROM material_number_union) a 
    LEFT JOIN (
        SELECT DISTINCT matnr, 
            FIRST_VALUE(pmatn) OVER (
                PARTITION BY matnr, VKORG 
                ORDER BY 
                    CASE 
                        WHEN vtweg != 19 AND pmatn IS NOT NULL AND TRIM(pmatn) != '' THEN 1
                        WHEN vtweg != 19 AND (pmatn IS NULL OR TRIM(pmatn) = '') THEN 2
                        ELSE 3 
                    END
            ) as pmatn,
            VKORG , 
            VTWEG 
        FROM apc_mvke
        QUALIFY ROW_NUMBER() OVER (PARTITION BY VKORG, MATNR ORDER BY VTWEG) = 1
    ) b
        ON ltrim(a.matl_num,'0') = ltrim(b.matnr,'0')
        AND a.org = b.vkorg
),

base_join AS (
    SELECT 
        A.KNUMH AS COND_REC_NO,
        A.KSCHL AS KNART,
        CASE
            WHEN A.KONWA IN ('IDR','JPY','KRW','VND') THEN A.KBETR*100
            ELSE A.KBETR
        END AS AMOUNT,
        A.KONWA AS CURRENCY,
        A.KPEIN AS PRICE_UNIT,
        A.KMEIN AS UNIT,
        B.VKORG AS SLS_ORG,
        B.MATNR AS MATERIAL,
        B.DATBI AS VALID_TO,
        B.DATAB AS DT_FROM
    FROM (SELECT KNUMH, KSCHL, KBETR, KONWA, KPEIN, KMEIN FROM APC_KONP) A
    JOIN (SELECT * FROM APC_A902) B
        ON A.KSCHL = B.KSCHL
        AND A.KNUMH = B.KNUMH
),


india_records AS (
    SELECT 
        SLS_ORG,
        MATERIAL,
        COND_REC_NO,
        VALID_TO,
        KNART,
        DT_FROM,
        AMOUNT,
        CURRENCY,
        UNIT,
        PRICE_UNIT
    FROM base_join
    WHERE TRIM(SLS_ORG) IN (
        SELECT TRIM(sls_org)
        FROM EDW_SALES_ORG_DIM
        WHERE ctry_key = 'IN' 
    ) AND KNART = 'ZPRD'
),
other_country_records AS (
    SELECT 
        SLS_ORG,
        MATERIAL,
        COND_REC_NO,
        VALID_TO,
        KNART,
        DT_FROM,
        AMOUNT,
        CURRENCY,
        UNIT,
        PRICE_UNIT
    FROM base_join
    WHERE 
    TRIM(SLS_ORG) IN (
        SELECT TRIM(sls_org)
        FROM EDW_SALES_ORG_DIM
        WHERE ctry_key != 'IN' 
    )  AND 
    KNART = 'ZPR0'
),

all_records AS
(
  SELECT *
  FROM india_records
  UNION ALL 
  SELECT *
  FROM other_country_records
),

mvke_join AS (
    SELECT  
        A.MATL_NUM,
        A.PMATN,
        A.VKORG,
        A.VTWEG,
        COALESCE(B.COND_REC_NO, C.COND_REC_NO) AS COND_REC_NO,
        COALESCE(B.KNART, C.KNART) AS KNART,
        COALESCE(B.AMOUNT, C.AMOUNT) AS AMOUNT,
        COALESCE(B.CURRENCY, C.CURRENCY) AS CURRENCY,
        COALESCE(B.PRICE_UNIT, C.PRICE_UNIT) AS PRICE_UNIT,
        COALESCE(B.UNIT, C.UNIT) AS UNIT,
        COALESCE(B.VALID_TO, C.VALID_TO) AS VALID_TO,
        COALESCE(B.DT_FROM, C.DT_FROM) AS DT_FROM,
        CASE 
            WHEN 
            B.COND_REC_NO IS NOT NULL and
            B.KNART IS NOT NULL and
            B.AMOUNT IS NOT NULL and
            B.PRICE_UNIT IS NOT NULL and
            B.UNIT IS NOT NULL and
            B.VALID_TO IS NOT NULL and
            B.DT_FROM IS NOT NULL 
            THEN 'Y'
            ELSE 'N'
        END AS pmatn_flag
    FROM (SELECT * FROM MATERIAL_JOIN
    -- QUALIFY ROW_NUMBER() OVER (
    --     PARTITION BY VKORG, MATL_NUM
    --     ORDER BY VTWEG)= 1
    ) A
    LEFT JOIN (
        SELECT * 
        FROM all_records 
        WHERE valid_to != '00000000' 
        OR dt_from != '00000000'
    ) B
        ON A.PMATN = B.MATERIAL
        AND A.VKORG = B.SLS_ORG
    LEFT JOIN (
        SELECT * 
        FROM all_records 
        WHERE valid_to != '00000000' 
        OR dt_from != '00000000'
    ) C
        ON A.MATL_NUM = C.MATERIAL
        AND A.VKORG = C.SLS_ORG
),

FINAL AS (
    SELECT 
        m.MATL_NUM AS material,
        m.PMATN,
        -- m.vtweg as channel,
        m.VKORG AS sls_org,
        m.COND_REC_NO,
        m.VALID_TO,
        m.KNART,
        m.DT_FROM,
        CAST(m.AMOUNT AS DECIMAL(20,4)) AS amount,
        m.CURRENCY,
        m.UNIT,
        m.PRICE_UNIT,
        CURRENT_TIMESTAMP()::timestamp_ntz(9) AS crtd_dttm,
        CURRENT_TIMESTAMP()::timestamp_ntz(9) AS updt_dttm,
        m.pmatn_flag,
        NULL AS file_name
    FROM mvke_join m
)

SELECT DISTINCT *
FROM FINAL
WHERE COND_REC_NO IS NOT NULL