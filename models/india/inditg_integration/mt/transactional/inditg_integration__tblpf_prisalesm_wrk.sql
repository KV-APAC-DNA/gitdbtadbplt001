with 
edw_billing_fact as 
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
edw_retailer_calendar_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
itg_mds_month_end_dates as 
(
    select * from {{ ref('inditg_integration__itg_mds_month_end_dates') }}
),
temp as 
(
        SELECT DISTINCT b.serno,
            b.mon,
            b.yr,
            LTRIM(b.distcode, 0) AS distcode,
            LTRIM(b.prdcode, 0) AS prdcode,
            b.billtype,
            b.created_on,
            (
                SUM(b.qty) OVER (
                    PARTITION BY distcode,
                    prdcode,
                    billtype,
                    created_on
                )
            ) as qty,
            (
                SUM(b.value) OVER (
                    PARTITION BY distcode,
                    prdcode,
                    billtype,
                    created_on
                )
            ) as value
        FROM ( select * from 
                (
                    SELECT CAST(NULL AS BIGINT) AS serno,
                        sold_to AS distcode,
                        material AS prdcode,
                        bill_type AS billtype,
                        CAST(bill_qty AS NUMERIC(16, 4)) AS qty,
                        CAST(subtotal_4 AS NUMERIC(16, 4)) AS value,
                        created_on,
                        bill_dt
                    FROM edw_billing_fact
                    WHERE UPPER(bill_type) IN (
                            'S1',
                            'S2',
                            'ZC22',
                            'ZC2D',
                            'ZF2D',
                            'ZG22',
                            'ZG2D',
                            'ZL2D',
                            'ZL22',
                            'ZC3D',
                            'ZF2E',
                            'ZL3D',
                            'ZG3D',
                            'ZRSM',
                            'ZSMD'
                        )
                        AND sls_org = '5100'
                ) prisalem
                INNER JOIN (
                    SELECT DISTINCT customer_code
                    FROM edw_customer_dim
                    WHERE UPPER(psnonps) = 'Y'
                        OR (
                            UPPER(psnonps) = 'N'
                            AND UPPER(direct_account_flag) <> 'N'
                        )
                ) cust ON LTRIM(prisalem.distcode, '0') = cust.customer_code
                INNER JOIN (
                    SELECT CAST(SUBSTRING(mth_mm, 5, 2) AS INT) AS mon,
                        CAST(SUBSTRING(mth_mm, 1, 4) AS INT) AS yr,
                        caldate
                    FROM edw_retailer_calendar_dim
                    WHERE mth_mm IN (
                            SELECT (year || lpad (MONTH, 2, 0))
                            FROM itg_mds_month_end_dates
                            WHERE to_date(convert_timezone('Asia/Kolkata', current_timestamp())) <= to_date(pathfinder_month_end)
                        )
                ) cal_dim ON to_date(prisalem.bill_dt) = to_date(cal_dim.caldate)
                INNER JOIN (
                    SELECT YEAR,
                        MONTH,
                        pathfinder_month_end
                    FROM itg_mds_month_end_dates
                ) mon_end ON mon_end.year = cal_dim.yr
                AND mon_end.month = cal_dim.mon
                AND to_date(created_on) <= to_date(pathfinder_month_end)
            ) b
),
trans as 
(
    SELECT 
    serno,
    mon,
    yr,
    distcode,
    prdcode,
    billtype,
    SUM(qty) AS qty,
    SUM(value) AS value,
    SUM(value) / SUM(qty) AS nr,
    convert_timezone('Asia/Kolkata', current_timestamp()) AS crt_dttm,
    convert_timezone('Asia/Kolkata', current_timestamp()) AS updt_dttm
FROM temp
GROUP BY serno,
    mon,
    yr,
    distcode,
    prdcode,
    billtype
),
final as 
(
    select 
	serno::number(38,0) as serno,
	mon::number(18,0) as mon,
	yr::number(18,0) as yr,
	distcode::varchar(50) as distcode,
	prdcode::varchar(50) as prdcode,
	billtype::varchar(50) as billtype,
	qty::number(16,4) as qty,
	value::number(16,4) as value,
	nr::number(12,4) as nr,
	crt_dttm::timestamp_ntz(9) as crt_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final