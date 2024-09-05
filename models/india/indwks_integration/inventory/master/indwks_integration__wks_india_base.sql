with EDW_CALENDAR_DIM as
(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_CALENDAR_DIM
),
EDW_BILLING_FACT as
(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_BILLING_FACT
),
edw_retailer_calendar_dim as
(
    select * from DEV_DNA_CORE.INDEDW_INTEGRATION.EDW_RETAILER_CALENDAR_DIM
),
v_pf_sales_stock_inventory_analysis as
(
     select * from PROD_DNA_CORE.INDEDW_INTEGRATION.v_pf_sales_stock_inventory_analysis
),
trans as
(
SELECT
COUNTRY_NAME,
SOLD_TO,
MATL_NUM,
YEAR,
--QRTR_NO,
MNTH_ID,
--MNTH_NO,
SUM(SI_SLS_QTY) AS SI_SLS_QTY,
SUM(SI_GTS_VAL) AS SI_GTS_VAL,
SUM(CLOSING_STOCK) AS CLOSING_STOCK,
SUM(CLOSING_STOCK_VAL) AS CLOSING_STOCK_VAL,
SUM(SO_QTY) AS SO_QTY,
SUM(SO_VAL) AS SO_VAL

FROM 
(
WITH TIME AS
(
SELECT ECD.FISC_YR AS YEAR,
                                   CASE
                                                WHEN ECD.PSTNG_PER = 1 THEN 1
                                                WHEN ECD.PSTNG_PER = 2 THEN 1
                                                WHEN ECD.PSTNG_PER = 3 THEN 1
                                                WHEN ECD.PSTNG_PER = 4 THEN 2
                                                WHEN ECD.PSTNG_PER = 5 THEN 2
                                                WHEN ECD.PSTNG_PER = 6 THEN 2
                                                WHEN ECD.PSTNG_PER = 7 THEN 3
                                                WHEN ECD.PSTNG_PER = 8 THEN 3
                                                WHEN ECD.PSTNG_PER = 9 THEN 3
                                                WHEN ECD.PSTNG_PER = 10 THEN 4
                                                WHEN ECD.PSTNG_PER = 11 THEN 4
                                                WHEN ECD.PSTNG_PER = 12 THEN 4
                                                ELSE NULL
                                   END AS QRTR_NO,
                                   ECD.FISC_YR||'/'||CASE
                                                WHEN ECD.PSTNG_PER = 1 THEN 'Q1'
                                                WHEN ECD.PSTNG_PER = 2 THEN 'Q1'
                                                WHEN ECD.PSTNG_PER = 3 THEN 'Q1'
                                                WHEN ECD.PSTNG_PER = 4 THEN 'Q2'
                                                WHEN ECD.PSTNG_PER = 5 THEN 'Q2'
                                                WHEN ECD.PSTNG_PER = 6 THEN 'Q2'
                                                WHEN ECD.PSTNG_PER = 7 THEN 'Q3'
                                                WHEN ECD.PSTNG_PER = 8 THEN 'Q3'
                                                WHEN ECD.PSTNG_PER = 9 THEN 'Q3'
                                                WHEN ECD.PSTNG_PER = 10 THEN 'Q4'
                                                WHEN ECD.PSTNG_PER = 11 THEN 'Q4'
                                                WHEN ECD.PSTNG_PER = 12 THEN 'Q4'
                                                ELSE NULL
                                   END AS QRTR,
                                   ECD.FISC_YR||TRIM(TO_CHAR(ECD.PSTNG_PER,'00')) AS MNTH_ID,
                                   ECD.FISC_YR||'/'||CASE WHEN ECD.PSTNG_PER = 1 THEN 'JAN' WHEN ECD.PSTNG_PER = 2 THEN 'FEB' WHEN ECD.PSTNG_PER = 3 THEN 'MAR' WHEN ECD.PSTNG_PER = 4 THEN 'APR' WHEN ECD.PSTNG_PER = 5 THEN 'MAY' WHEN ECD.PSTNG_PER = 6 THEN 'JUN' WHEN ECD.PSTNG_PER = 7 THEN 'JUL' WHEN ECD.PSTNG_PER = 8 THEN 'AUG' WHEN ECD.PSTNG_PER = 9 THEN 'SEP' WHEN ECD.PSTNG_PER = 10 THEN 'OCT' WHEN ECD.PSTNG_PER = 11 THEN 'NOV' WHEN ECD.PSTNG_PER = 12 THEN 'DEC' ELSE NULL END AS MNTH_DESC,
                                   ECD.PSTNG_PER AS MNTH_NO,
                                   CASE
                                                WHEN ECD.PSTNG_PER = 1 THEN 'JAN'
                                                WHEN ECD.PSTNG_PER = 2 THEN 'FEB'
                                                WHEN ECD.PSTNG_PER = 3 THEN 'MAR'
                                                WHEN ECD.PSTNG_PER = 4 THEN 'APR'
                                                WHEN ECD.PSTNG_PER = 5 THEN 'MAY'
                                                WHEN ECD.PSTNG_PER = 6 THEN 'JUN'
                                                WHEN ECD.PSTNG_PER = 7 THEN 'JUL'
                                                WHEN ECD.PSTNG_PER = 8 THEN 'AUG'
                                                WHEN ECD.PSTNG_PER = 9 THEN 'SEP'
                                                WHEN ECD.PSTNG_PER = 10 THEN 'OCT'
                                                WHEN ECD.PSTNG_PER = 11 THEN 'NOV'
                                                WHEN ECD.PSTNG_PER = 12 THEN 'DEC'
                                                ELSE NULL
                                   END AS MNTH_SHRT,
                                   CASE
                                                WHEN ECD.PSTNG_PER = 1 THEN 'JANUARY'
                                                WHEN ECD.PSTNG_PER = 2 THEN 'FEBRUARY'
                                                WHEN ECD.PSTNG_PER = 3 THEN 'MARCH'
                                                WHEN ECD.PSTNG_PER = 4 THEN 'APRIL'
                                                WHEN ECD.PSTNG_PER = 5 THEN 'MAY'
                                                WHEN ECD.PSTNG_PER = 6 THEN 'JUNE'
                                                WHEN ECD.PSTNG_PER = 7 THEN 'JULY'
                                                WHEN ECD.PSTNG_PER = 8 THEN 'AUGUST'
                                                WHEN ECD.PSTNG_PER = 9 THEN 'SEPTEMBER'
                                                WHEN ECD.PSTNG_PER = 10 THEN 'OCTOBER'
                                                WHEN ECD.PSTNG_PER = 11 THEN 'NOVEMBER'
                                                WHEN ECD.PSTNG_PER = 12 THEN 'DECEMBER'
                                                ELSE NULL
                                   END AS MNTH_LONG,
                                  CYRWKNO.YR_WK_NUM AS WK,
                                   CMWKNO.MNTH_WK_NUM AS MNTH_WK_NO,
                                   ECD.CAL_YR AS CAL_YEAR,
                                   ECD.CAL_QTR_1 AS CAL_QRTR_NO,
                                   ECD.CAL_MO_1 AS CAL_MNTH_ID,
                                   ECD.CAL_MO_2 AS CAL_MNTH_NO,
                                   CASE
                                                WHEN ECD.CAL_MO_2 = 1 THEN 'JANUARY'
                                                WHEN ECD.CAL_MO_2 = 2 THEN 'FEBRUARY'
                                                WHEN ECD.CAL_MO_2 = 3 THEN 'MARCH'
                                                WHEN ECD.CAL_MO_2 = 4 THEN 'APRIL'
                                                WHEN ECD.CAL_MO_2 = 5 THEN 'MAY'
                                                WHEN ECD.CAL_MO_2 = 6 THEN 'JUNE'
                                                WHEN ECD.CAL_MO_2 = 7 THEN 'JULY'
                                                WHEN ECD.CAL_MO_2 = 8 THEN 'AUGUST'
                                                WHEN ECD.CAL_MO_2 = 9 THEN 'SEPTEMBER'
                                                WHEN ECD.CAL_MO_2 = 10 THEN 'OCTOBER'
                                                WHEN ECD.CAL_MO_2 = 11 THEN 'NOVEMBER'
                                                WHEN ECD.CAL_MO_2 = 12 THEN 'DECEMBER'
                                                ELSE NULL
                                   END AS CAL_MNTH_NM,
                                   DATE_TRUNC('DAY', CAST(ECD.CAL_DAY AS TIMESTAMP)) AS CAL_DATE,
                                   --TRUNC(CAST(ECD.CAL_DAY AS TIMESTAMP)) AS CAL_DATE,
                                   REPLACE(ECD.CAL_DAY,'-','') AS CAL_DATE_ID
                                FROM EDW_CALENDAR_DIM AS ECD,
                                (SELECT ROW_NUMBER() OVER (PARTITION BY A.FISC_PER ORDER BY A.CAL_WK) AS MNTH_WK_NUM,
                                                                DATE_TRUNC('DAY', DATEADD('DAY', -6, A.CAL_DAY)) AS CAL_DAY_FIRST,
                                                                --DATE_TRUNC(DATEADD ('DAY',-6,A.CAL_DAY)) AS CAL_DAY_FIRST,
                                                                A.CAL_DAY AS CAL_DAY_LAST
                                  FROM EDW_CALENDAR_DIM A
                                  WHERE (A.CAL_DAY IN (SELECT EDW_CALENDAR_DIM.CAL_DAY
                                                                                                                   FROM EDW_CALENDAR_DIM
                                                                                                                   WHERE EDW_CALENDAR_DIM.WKDAY = 7))
                                  ORDER BY A.CAL_WK) AS CMWKNO,
                                (SELECT ROW_NUMBER() OVER (PARTITION BY A.FISC_YR ORDER BY A.CAL_WK) AS YR_WK_NUM,
                                                               DATE_TRUNC('DAY', DATEADD('DAY', -6, A.CAL_DAY)) AS CAL_DAY_FIRST,
                                                                --DATE_TRUNC(DATEADD ('DAY',-6,A.CAL_DAY)) AS CAL_DAY_FIRST,
                                                                A.CAL_DAY AS CAL_DAY_LAST
                                  FROM EDW_CALENDAR_DIM A
                                  WHERE (A.CAL_DAY IN (SELECT EDW_CALENDAR_DIM.CAL_DAY
                                                                                                                   FROM EDW_CALENDAR_DIM
                                                                                                                   WHERE EDW_CALENDAR_DIM.WKDAY = 7))
                                  ORDER BY A.CAL_WK) AS CYRWKNO
                WHERE ECD.CAL_DAY BETWEEN CMWKNO.CAL_DAY_FIRST AND CMWKNO.CAL_DAY_LAST
                AND   ECD.CAL_DAY BETWEEN CYRWKNO.CAL_DAY_FIRST AND CYRWKNO.CAL_DAY_LAST)

SELECT 
--TO_CHAR(T1.BILL_DT,'YYYY-MM-DD') AS BILL_DT,
'SELLIN' AS DATA_TYPE,  
'India' AS COUNTRY_NAME, 
T1.SOLD_TO,
T1.MATL_NUM,
T2.fisc_yr as year,
CAST(T2.qtr as VARCHAR(14)) as QRTR_NO , 
CAST (T2.mth_mm AS  VARCHAR(21)) as MNTH_ID ,
T2.mth_yyyymm as MNTH_NO,
bill_qty_pc AS SI_SLS_QTY,
grs_trd_sls AS SI_GTS_VAL,
0 as CLOSING_STOCK,
0 as  CLOSING_STOCK_VAL,
0 AS SO_QTY,
0 as SO_VAL
FROM 
(SELECT BILL_DT,LTRIM(SOLD_TO,'0') AS SOLD_TO,LTRIM(MATERIAL,'0') AS MATL_NUM,BILL_TYPE, SUM(BILL_QTY) AS BILL_QTY_PC,
       SUM(subtotal_4) AS GRS_TRD_SLS FROM EDW_BILLING_FACT WHERE SLS_ORG IN ('5100') 
	   AND BILL_TYPE in ('S1','S2','ZC22','ZC2D','ZF2D','ZG22','ZG2D','ZL2D','ZL22','ZRSM','ZSMD','ZC3D','ZF2E','ZL3D','ZG3D') 
	   AND 
       --CAST(TO_CHAR(TRUNC(BILL_DT),'YYYY') AS INT)
       CAST(TO_CHAR(DATE_TRUNC('YEAR', BILL_DT), 'YYYY') AS INT)>=(date_part(year, convert_timezone('UTC', current_timestamp())) -6)
                   GROUP BY BILL_DT,BILL_TYPE,SOLD_TO,MATL_NUM ) T1,
edw_retailer_calendar_dim T2
WHERE 
DATE_TRUNC('DAY', T1.BILL_DT) = DATE_TRUNC('DAY', T2.caldate)
--TRUNC(T1.BILL_DT)=TRUNC(T2.caldate)

UNION ALL

SELECT 
'INVENTORY & SELLOUT' AS DATA_TYPE,  
'India' AS COUNTRY_NAME, 
CLS.customer_code AS SOLD_TO,
CLS.product_code AS MATL_NUM,
TIME.YEAR,
TIME.QRTR_NO,
TIME.MNTH_ID,
TIME.MNTH_NO,
0 AS SI_SLS_QTY,
0 AS SI_GTS_VAL,
CLS.cl_stck_qty AS CLOSING_STOCK,
CLS.cl_stck_nr_value AS  CLOSING_STOCK_VAL,
CLS.sec_prd_qty AS SO_QTY,
SO_VAL
--SELECT *
FROM (
select 
year,
mth_mm,
customer_code,
product_code,
sec_prd_qty,
cl_stck_qty,
cl_stck_value_nr,
(cl_stck_qty*cl_stck_nr) as cl_stck_nr_value,
(sec_prd_nr_value +sec_prd_nr_value_ret)SO_VAL
from 
v_pf_sales_stock_inventory_analysis
where year >=(date_part(year, convert_timezone('UTC', current_timestamp())) -6)
) AS CLS, 
(SELECT DISTINCT A.fisc_yr as YEAR,
CAST(A.qtr AS VARCHAR(14)) AS QRTR_NO , 
CAST (A.mth_mm AS  VARCHAR(21)) AS MNTH_ID ,
A.mth_yyyymm as MNTH_NO 
FROM edw_retailer_calendar_dim A) AS TIME
WHERE CLS.mth_mm IS NOT NULL
AND (CLS.mth_mm)=(TIME.MNTH_ID)   
)

GROUP BY COUNTRY_NAME,
SOLD_TO,
MATL_NUM,
YEAR,
MNTH_ID
),
final as
(
    select 
    country_name::varchar(5) as country_name,
	sold_to::varchar(50) as sold_to,
	matl_num::varchar(50) as matl_num,
	year::number(38,0) as year,
	mnth_id::varchar(21) as mnth_id,
	si_sls_qty::number(38,4) as si_sls_qty,
	si_gts_val::number(38,4) as si_gts_val,
	closing_stock::number(38,3) as closing_stock,
	closing_stock_val::number(38,6) as closing_stock_val,
	so_qty::number(38,4) as so_qty,
	so_val::number(38,3) as so_val
    from trans
)
select * from final