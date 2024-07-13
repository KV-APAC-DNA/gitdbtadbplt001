with edw_billing_fact as
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
itg_rpurchasedetail as
(
    select * from {{ ref('inditg_integration__itg_rpurchasedetail') }}
),
itg_customer as
(
    select * from {{ source('inditg_integration', 'itg_customer') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
itg_mds_month_end_dates as
(
    select * from {{ ref('inditg_integration__itg_mds_month_end_dates') }}
),
final as
(
    SELECT DISTINCT 
    a.serno,
    a.distcode,
    a.prdcode,
    a.month,
    a.year,
    a.subdcode,
    a.sitqty,
    a.sitvalue,
    a.src,
    a.createddate,
    convert_timezone('Asia/Kolkata',current_timestamp()) AS crt_dttm,
    convert_timezone('Asia/Kolkata',current_timestamp()) AS updt_dttm
    FROM (SELECT DISTINCT sit.serno,
        LTRIM(sit.distcode,0) AS distcode,
        LTRIM(sit.prdcode,0) AS prdcode,
        cal_dim.mon AS MONTH,
        cal_dim.yr AS YEAR,
        NULL AS subdcode,
        SUM(sit.sitqty) OVER (PARTITION BY distcode,prdcode,created_on) AS sitqty,
        SUM(sit.sitvalue) OVER (PARTITION BY distcode,prdcode,created_on) AS sitvalue,
        'PWS' AS src,
        sit.created_on AS createddate
        FROM (SELECT CAST(NULL AS BIGINT) AS serno,
                sold_to AS distcode,
                material AS prdcode,
                bill_qty AS sitqty,
                subtotal_4 AS sitvalue,
                created_on,
                bill_dt,
                bill_num
        FROM edw_billing_fact
        WHERE bill_num IN (SELECT DISTINCT CompInvno FROM itg_rpurchasedetail)
        AND   sls_org = '5100') sit
    INNER JOIN (SELECT DISTINCT sapid
                FROM itg_customer
                WHERE UPPER(psnonps) = 'Y'
                AND   UPPER(isactive) = 'Y') cust ON ltrim(sit.distcode,'0') = cust.sapid
    INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                        CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                        caldate
                FROM edw_retailer_calendar_dim
                WHERE mth_mm IN (SELECT (year||lpad (MONTH,2,0))
                                FROM itg_mds_month_end_dates
                                WHERE TO_DATE(convert_timezone('Asia/Kolkata',current_timestamp())) <= TO_DATE(pathfinder_month_end)
                                )) cal_dim ON TO_DATE (sit.bill_dt) = TO_DATE (cal_dim.caldate)) a
)
select * from final