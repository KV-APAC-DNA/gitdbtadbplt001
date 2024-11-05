

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
base_join AS
(
  SELECT A.KNUMH AS COND_REC_NO,
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
         B.DATAB AS DT_FROM,
         -- C.CTRY_KEY
         --C.SLS_ORG
         --B.DELETED
         FROM (SELECT KNUMH, KSCHL, KBETR, KONWA, KPEIN, KMEIN FROM APC_KONP) A
    JOIN (SELECT * FROM APC_A902) B
      ON A.KSCHL = B.KSCHL
     AND A.KNUMH = B.KNUMH
),
india_records AS
(
  SELECT SLS_ORG,
         MATERIAL,
         COND_REC_NO,
         VALID_TO,
         'ZPRD' AS KNART,
         DT_FROM,
         AMOUNT,
         CURRENCY,
         UNIT,
         PRICE_UNIT,
         FROM base_join
  WHERE TRIM(SLS_ORG) IN (SELECT TRIM(sls_org)
                           FROM EDW_SALES_ORG_DIM
                           WHERE ctry_key = 'IN')
),
other_country_records AS
(
  SELECT SLS_ORG,
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
  WHERE TRIM(SLS_ORG) IN (SELECT TRIM(sls_org)
                           FROM EDW_SALES_ORG_DIM
                           WHERE ctry_key NOT IN ('IN'))
),
all_records AS
(
  SELECT *
  FROM india_records
  UNION ALL
  SELECT *
  FROM other_country_records
),
mvke_join AS
(
  SELECT all_records.*,
         b.pmatn
  FROM all_records
    LEFT JOIN (SELECT distinct matnr, pmatn,VKORG, VTWEG, row_number() over (partition by matnr,VKORG order by pmatn desc, vtweg desc) as rn 
    FROM                             
    APC_MVKE WHERE
                PMATN IS NOT NULL AND PMATN <>''
                AND VTWEG in (10,11,15) ) b
           ON all_records.material = B.MATNR
           AND all_records.sls_org=b.vkorg AND b.rn=1
),
final AS
(
  SELECT mvke_join.sls_org::VARCHAR(50) AS sls_org,
         mvke_join.material::VARCHAR(50) AS material,
         mvke_join.cond_rec_no::VARCHAR(100) AS cond_rec_no,
         --   mvke_join.matl_grp::VARCHAR(100) as matl_grp,
         mvke_join.valid_to::VARCHAR(20) AS valid_to,
         mvke_join.knart::VARCHAR(20) AS knart,
         mvke_join.dt_from::VARCHAR(20) AS dt_from,
         CAST(mvke_join.amount AS DECIMAL(20,4)) AS amount,
         mvke_join.currency::VARCHAR(20) AS currency,
         mvke_join.unit::VARCHAR(20) AS unit,
         --these 2 are null
         --   mvke_join.record_mode::VARCHAR(100) as record_mode,
         --   mvke_join.comp_cd::VARCHAR(100) as comp_cd,
         mvke_join.price_unit::VARCHAR(50) AS price_unit,
         --   mvke_join.zcurrfpa::VARCHAR(20) as zcurrfpa,
         --   mvke_join.cdl_dttm::VARCHAR(255) as cdl_dttm,
         mvke_join.pmatn,
         CURRENT_TIMESTAMP()::timestamp_ntz(9) AS crtd_dttm,
         CURRENT_TIMESTAMP()::timestamp_ntz(9) AS updt_dttm,
         NULL AS file_name
  FROM mvke_join
        {% if is_incremental() %}
  -- this filter will only be applied on an incremental run
        LEFT JOIN {{this}} AS itg
           ON TRIM (mvke_join.sls_org) = TRIM (itg.sls_org)
          AND LTRIM (mvke_join.material,0) = LTRIM (itg.material,0)
          AND mvke_join.cond_rec_no = itg.cond_rec_no
          AND TO_DATE (mvke_join.valid_to,'YYYYMMDD') = TO_DATE (itg.valid_to,'YYYYMMDD')
          AND TO_DATE (mvke_join.dt_from,'YYYYMMDD') = TO_DATE (itg.dt_from,'YYYYMMDD')
          AND mvke_join.amount = itg.amount
  WHERE itg.material IS NULL
   {% endif %}
)
SELECT A.*, 
B.unit as uom_unit,
b.base_uom,
b.uomz1d,
b.uomn1d
from final A 
LEFT JOIN EDW_MATERIAL_UOM b
on rtrim(a.material)=rtrim(b.material) and a.unit = b.unit
