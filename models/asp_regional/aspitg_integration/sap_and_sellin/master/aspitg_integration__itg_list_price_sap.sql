

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

-- Material consolidation CTEs
material_number_union AS (
    SELECT DISTINCT matnr AS matl_num FROM apc_mvke
    UNION
    SELECT DISTINCT PMATN AS matl_num FROM apc_mvke
    UNION
    SELECT DISTINCT matnr AS matl_num FROM apc_a902
    UNION 
    SELECT DISTINCT matl_num FROM edw_material_dim
),
material_join AS (
    SELECT A.*, b.* 
    FROM (SELECT DISTINCT MATL_NUM FROM material_number_union) a 
    LEFT JOIN (SELECT DISTINCT matnr, pmatn, VKORG, VTWEG FROM apc_mvke) b
        ON ltrim(a.matl_num,'0') = ltrim(b.matnr,'0')
),

-- Base pricing CTEs
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

-- Country specific records
india_records AS (
    SELECT 
        SLS_ORG,
        MATERIAL,
        COND_REC_NO,
        VALID_TO,
        'ZPRD' AS KNART,
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
    )
),
other_country_records AS (
    SELECT 
        SLS_ORG,
        MATERIAL,
        COND_REC_NO,
        VALID_TO,
        'ZPR0' AS KNART,
        DT_FROM,
        AMOUNT,
        CURRENCY,
        UNIT,
        PRICE_UNIT
    FROM base_join
    WHERE TRIM(SLS_ORG) IN (
        SELECT TRIM(sls_org)
        FROM EDW_SALES_ORG_DIM
        WHERE ctry_key != 'IN'
    )
),
all_records AS (
    SELECT * FROM india_records
    UNION ALL
    SELECT * FROM other_country_records
),

mvke_join AS (
    SELECT A.*, B.*,CASE WHEN B.COND_REC_NO IS NULL AND B.AMOUNT IS NULL THEN 'Y' ELSE 'N' END AS pmatn_flag 
    FROM (SELECT * FROM MATERIAL_JOIN) a
    LEFT JOIN (
        SELECT * 
        FROM all_records 
        WHERE valid_to != '00000000' 
        OR dt_from != '00000000'
    ) b
        ON A.MATL_NUM = B.MATERIAL
        AND A.VKORG = B.SLS_ORG
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY A.VKORG, A.MATL_NUM, B.COND_REC_NO 
        ORDER BY A.VTWEG
    ) = 1
),

FINAL AS (
    SELECT m.MATL_NUM,M.PMATN,
        CASE WHEN m.pmatn_flag = 'Y' THEN b.sls_org ELSE m.sls_org END::VARCHAR(50) AS sls_org,
        CASE WHEN m.pmatn_flag = 'Y' THEN b.material ELSE m.material END::VARCHAR(50) AS material,
        CASE WHEN m.pmatn_flag = 'Y' THEN b.cond_rec_no ELSE m.cond_rec_no END::VARCHAR(100) AS cond_rec_no,
        CASE WHEN m.pmatn_flag = 'Y' THEN b.valid_to ELSE m.valid_to END::VARCHAR(20) AS valid_to,
        CASE WHEN m.pmatn_flag = 'Y' THEN b.knart ELSE m.knart END::VARCHAR(20) AS knart,
        CASE WHEN m.pmatn_flag = 'Y' THEN b.dt_from ELSE m.dt_from END::VARCHAR(20) AS dt_from,
        CASE WHEN m.pmatn_flag = 'Y' THEN CAST(b.amount AS DECIMAL(20,4)) ELSE CAST(m.amount AS DECIMAL(20,4)) END AS amount,
        CASE WHEN m.pmatn_flag = 'Y' THEN b.currency ELSE m.currency END::VARCHAR(20) AS currency,
        CASE WHEN m.pmatn_flag = 'Y' THEN b.unit ELSE m.unit END::VARCHAR(20) AS unit,
        CASE WHEN m.pmatn_flag = 'Y' THEN b.price_unit ELSE m.price_unit END::VARCHAR(50) AS price_unit,
        -- CASE WHEN m.pmatn_flag = 'Y' THEN b.pmatn ELSE m.pmatn END AS pmatn,
        CURRENT_TIMESTAMP()::timestamp_ntz(9) AS crtd_dttm,
        CURRENT_TIMESTAMP()::timestamp_ntz(9) AS updt_dttm,
        m.pmatn_flag,
        NULL AS file_name

    FROM (
        SELECT *            
        FROM mvke_join 
    )m
    LEFT JOIN mvke_join b ON m.pmatn = b.MATL_NUM and m.vkorg = b.sls_org
    AND m.amount is null
)

SELECT DISTINCT *
FROM FINAL WHERE 
COND_REC_NO is not null