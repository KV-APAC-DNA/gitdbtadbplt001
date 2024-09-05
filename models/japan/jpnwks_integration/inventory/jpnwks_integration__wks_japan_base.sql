with EDW_BILLING_FACT as(
    select * from {{ ref('aspedw_integration__EDW_BILLING_FACT') }}
),
itg_parameter_reg_inventory as(
    select * from {{ ref('aspitg_integration__itg_parameter_reg_inventory') }}
),
EDW_VW_OS_TIME_DIM as(
    select * from {{ ref('sgpedw_integration__EDW_VW_OS_TIME_DIM') }}
),
dw_iv_month_end as(
    select * from {{ ref('jpnedw_integration__dw_iv_month_end') }}
),
edi_item_m as(
    select * from {{ ref('jpnedw_integration__edi_item_m') }}
),
DM_INTEGRATION_DLY as(
    select * from {{ ref('jpnedw_integration__DM_INTEGRATION_DLY') }}
),
VW_JAN_CHANGE as(
    select * from {{ ref('jpnedw_integration__VW_JAN_CHANGE') }}
),
cstm_m as(
    select * from {{ ref('jpnedw_integration__cstm_m') }}   
),
SISO as (
  with SI_SO_INV AS (
    SELECT
      COUNTRY_NAME,
      SOLD_TO,
      MATL_NUM,
      CAL_YEAR as YEAR,
      CAL_QRTR_NO as QRTR_NO,
      CAL_MNTH_ID as MNTH_ID,
      CAL_MNTH_NO as MNTH_NO,
      SUM(SI_SLS_QTY) AS SI_SLS_QTY,
      SUM(SI_GTS_VAL) AS SI_GTS_VAL,
      SUM(CLOSING_STOCK) AS CLOSING_STOCK,
      SUM(CLOSING_STOCK_VAL) AS CLOSING_STOCK_VAL,
      SUM(SO_QTY) AS SO_QTY,
      SUM(SO_VAL) AS SO_VAL
    FROM
      (
        SELECT
          'SELLIN' AS DATA_TYPE,
          'Japan' AS COUNTRY_NAME,
          T1.SOLD_TO,
          T1.MATL_NUM,
          T2.CAL_YEAR,
          CAST(T2.CAL_QRTR_NO as VARCHAR(14)) as CAL_QRTR_NO,
          CAST (T2.CAL_MNTH_ID AS VARCHAR(21)) as CAL_MNTH_ID,
          T2.CAL_MNTH_NO,
          bill_qty_pc AS SI_SLS_QTY,
          grs_trd_sls AS SI_GTS_VAL,
          0 as CLOSING_STOCK,
          0 as CLOSING_STOCK_VAL,
          0 AS SO_QTY,
          0 as SO_VAL
        FROM
          (
            SELECT
              BILL_DT,
              LTRIM(SOLD_TO, '0') AS SOLD_TO,
              LTRIM(MATERIAL, '0') AS MATL_NUM,
              BILL_TYPE,
              SUM(BILL_QTY) AS BILL_QTY_PC,
              SUM(SUBTOTAL_1) AS GRS_TRD_SLS
            FROM
              EDW_BILLING_FACT
            WHERE
              SLS_ORG IN (
                select
                  parameter_value
                from
                  itg_parameter_reg_inventory
                where
                  country_name = 'Japan'
              )
              AND BILL_TYPE = 'ZF2J'
              AND CAST(TO_CHAR(to_date(BILL_DT), 'YYYY') AS INT) >= (
                date_part(year, convert_timezone('UTC', current_timestamp())) -2
              )
            GROUP BY
              BILL_DT,
              BILL_TYPE,
              SOLD_TO,
              MATL_NUM
          ) T1,
          EDW_VW_OS_TIME_DIM T2
        WHERE
          to_date(T1.BILL_DT) = to_date(T2.CAL_DATE)
        UNION ALL
        SELECT
          'INVENTORY' AS DATA_TYPE,
          'Japan' AS COUNTRY_NAME,
          CLS.cstm_cd AS SOLD_TO,
          CLS.item_cd AS MATL_NUM,
          TIME.CAL_YEAR,
          TIME.CAL_QRTR_NO,
          TIME.CAL_MNTH_ID,
          TIME.CAL_MNTH_NO,
          0 AS SI_SLS_QTY,
          0 AS SI_GTS_VAL,
          CLS.qty AS CLOSING_STOCK,
          (NVL(CLS.qty, 0) * ITEM.unt_prc) AS CLOSING_STOCK_VAL,
          0 AS SO_QTY,
          0 AS SO_VAL
        FROM
          (
            SELECT
              T1.*,
              (
                RANK() OVER (
                  PARTITION BY T1.cstm_cd,
                  to_date(T1.invt_dt),
                  T1.item_cd
                  ORDER BY
                    T1.update_dt DESC
                )
              ) AS RANK
            FROM
              dw_iv_month_end T1 --WHERE to_date(T1.TRANSDATE)= '2019-05-26' AND T1.PRDCODE='26207811'
              --AND T1.DISTCODE='114981'
          ) AS CLS,
          (
            SELECT
              DISTINCT A.CAL_YEAR,
              CAST(A.CAL_QRTR_NO AS VARCHAR(14)) AS CAL_QRTR_NO,
              CAST (A.CAL_MNTH_ID AS VARCHAR(21)) AS CAL_MNTH_ID,
              A.CAL_MNTH_NO,
              MIN(CAL_DATE) OVER (PARTITION BY CAL_MNTH_ID) AS STRT_DT,
              MAX(CAL_DATE) OVER (PARTITION BY CAL_MNTH_ID) AS END_DATE
            FROM
              EDW_VW_OS_TIME_DIM A
          ) AS TIME,
          (
            select
              distinct item_cd,
              unt_prc
            from
              edi_item_m
          ) as ITEM
        WHERE
          CLS.invt_dt IS NOT NULL
          AND to_date(CLS.invt_dt) = to_date(TIME.END_DATE)
          and CAST(ITEM.item_cd(+) as varchar) = cast(CLS.item_cd as varchar)
          AND RANK = 1
          AND CAL_YEAR >= (
            date_part(year, convert_timezone('UTC', current_timestamp())) -2
          )
        UNION ALL
        SELECT
          'Sellout' AS DATA_TYPE,
          'Japan' AS COUNTRY_NAME,
          SO.JCP_CSTM_CD AS SOLD_TO,
          JAN.ITEM_CD AS MATL_NUM,
          TIME.CAL_YEAR,
          TIME.CAL_QRTR_NO,
          TIME.CAL_MNTH_ID,
          TIME.CAL_MNTH_NO,
          0 AS SI_SLS_QTY,
          0 AS SI_GTS_VAL,
          0 AS CLOSING_STOCK,
          0 AS CLOSING_STOCK_VAL,
          SO.JCP_QTY AS SO_QTY,
          (NVL(SO.JCP_QTY, 0) * ITEM.unt_prc) AS SO_VAL
        FROM
          (
            SELECT
              JCP_DATE,
              JCP_CSTM_CD,
              JCP_JAN_CD,
              JCP_QTY,
              JCP_AMT
            FROM
              DM_INTEGRATION_DLY
            WHERE
              JCP_DATA_SOURCE = 'SO'
          ) AS SO,
          (
            SELECT
              DISTINCT A.CAL_YEAR,
              CAST(A.CAL_QRTR_NO AS VARCHAR(14)) AS CAL_QRTR_NO,
              CAST (A.CAL_MNTH_ID AS VARCHAR(21)) AS CAL_MNTH_ID,
              A.CAL_MNTH_NO,
              A.CAL_DATE
            FROM
              EDW_VW_OS_TIME_DIM A
          ) AS TIME,
          VW_JAN_CHANGE AS JAN,
          (
            select
              distinct ITEM_CD,
              UNT_PRC
            from
              EDI_ITEM_M
          ) as ITEM
        WHERE
          to_date(SO.JCP_DATE) = to_date(TIME.CAL_DATE)
          AND JAN.JAN_CD(+) = SO.JCP_JAN_CD
          AND CAST(ITEM.item_cd(+) as varchar) = cast(JAN.ITEM_CD as varchar)
          AND TIME.CAL_YEAR >= (
            date_part(year, convert_timezone('UTC', current_timestamp())) -2
          )
      )
    group by
      COUNTRY_NAME,
      SOLD_TO,
      MATL_NUM,
      CAL_YEAR,
      CAL_QRTR_NO,
      CAL_MNTH_ID,
      CAL_MNTH_NO
  )
  Select
    country_name,
    t2.rebate_rep_cd as prnt_key,
    cstm_grp_nm as prnt_cust_desc,
    matl_num,
    year,
    qrtr_no,
    mnth_id,
    mnth_no,
    si_sls_qty,
    si_gts_val,
    closing_stock,
    closing_stock_val,
    so_qty,
    so_val
  from
    (
      Select
        *
      from
        SI_SO_INV
    ) t1,
    (
      select
        cust.cstm_cd,
        cust.cstm_nm,
        nvl(cust.rebate_rep_cd, 'NA') as rebate_rep_cd,
        nvl(cust_grp.cstm_nm, 'NA') as cstm_grp_nm
      from
        (
          select
            cstm_cd,
            cstm_nm,
            rebate_rep_cd
          from
            cstm_m
        ) cust,
        (
          select
            cstm_cd,
            cstm_nm
          from
            cstm_m
        ) cust_grp
      where
        cust.rebate_rep_cd = cust_grp.cstm_cd (+)
    ) t2
  where
    t1.sold_to = t2.cstm_cd
)
Select
  country_name,
  prnt_key,
  prnt_cust_desc,
  matl_num,
  year,
  mnth_id,
  Sum(si_sls_qty) si_sls_qty,
  sum(si_gts_val) si_gts_val,
  sum(closing_stock) closing_stock,
  sum(closing_stock_val) closing_stock_val,
  sum(so_qty) so_qty,
  sum(so_val) so_val
from
  SISO
group by
  country_name,
  prnt_key,
  prnt_cust_desc,
  matl_num,
  year,
  mnth_id