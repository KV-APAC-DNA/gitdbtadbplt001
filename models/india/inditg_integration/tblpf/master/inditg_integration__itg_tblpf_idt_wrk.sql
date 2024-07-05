with itg_rtbl_idtmanagementupload as
(
    select * from {{ ref('inditg_integration__itg_rtbl_idtmanagementupload') }}
),
itg_customer as
(
    select * from {{ source('inditg_integration', 'itg_customer') }}
),
itg_pricelist as
(
    select * from {{ ref('inditg_integration__itg_pricelist') }}
),
itg_mds_month_end_dates as
(
    select * from {{ ref('inditg_integration__itg_mds_month_end_dates') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
final as
(
    SELECT DISTINCT 
    a.serno,
    a.distcode,
    a.prdcode,
    a.idtmngdate,
    a.stkmgmttype,
    a.status,
    a.stocktype,
    a.baseqty,
    a.mrp,
    a.listprice,
    a.nr,
    a.ptr,
    a.month,
    a.year,
    a.src,
    convert_timezone('Asia/Kolkata',current_timestamp()) AS crt_dttm,
    convert_timezone('Asia/Kolkata',current_timestamp()) AS updt_dttm
    FROM (SELECT DISTINCT idt.serno,
        LTRIM(idt.distcode,0) AS distcode,
        LTRIM(idt.prdcode,0) AS prdcode,
        idt.idtmngdate,
        idt.stkmgmttype,
        idt.stocktype,
        nvl(idt.baseqty,0) AS baseqty,
        idt.mrp,
        idt.listprice,
        price.nr AS nr,
        price.ptr AS ptr,
        cal_dim.mon AS MONTH,
        cal_dim.yr AS YEAR,
        CASE
            WHEN UPPER(psnonps) = 'Y' THEN 'PWS'
            WHEN UPPER(psnonps) = 'N' THEN 'S'
            ELSE 'NA'
        END AS src,
        idt.status
        FROM (SELECT CAST(NULL AS BIGINT) AS serno,
                distcode,
                productcode AS prdcode,
                idtmngdate,
                stkmgmttype,
                status,
                CASE
                WHEN UPPER(StockType) = 'MGSALEABLE' THEN 'salable'
                ELSE (
                    CASE
                    WHEN UPPER(Stocktype) = 'MGUNSALEABLE' THEN 'unsalable'
                    ELSE 'offer'
                    END )
                END AS stocktype,
                SUM(nvl (baseqty,0)) OVER (PARTITION BY distcode,productcode,stkmgmttype,stocktype,idtmngdate,mrp,listprice,status) AS baseqty,
                mrp,
                listprice
        FROM itg_rtbl_idtmanagementupload) idt
    INNER JOIN (SELECT DISTINCT sapid,
                        psnonps,
                        statecode
                FROM itg_customer
                WHERE UPPER(isactive) = 'Y') cust ON LTRIM (idt.distcode,'0') = cust.sapid
    INNER JOIN (SELECT productcode,
                        statecode,
                        nr,
                        ptr,
                        startdate,
                        insertedon,
                        mrpperpack
                FROM (SELECT DISTINCT productcode,
                            statecode,
                            CASE
                                WHEN billingunit = 0 THEN 0
                                ELSE CAST(nr / billingunit AS NUMERIC(16,3))
                            END AS nr,
                            CASE
                                WHEN billingunit = 0 THEN 0
                                ELSE CAST(retailerprice / billingunit AS NUMERIC(16,3))
                            END AS ptr,
                            startDate,
                            insertedOn,
                            MrpPerPack,
                            ROW_NUMBER() OVER (PARTITION BY productcode,statecode,MrpPerPack ORDER BY startDate DESC,insertedOn DESC) AS rk
                    FROM itg_pricelist)
                WHERE rk = 1) price
            ON CASE WHEN idt.mrp <> '0' THEN LTRIM (idt.prdcode,0) = LTRIM (price.productcode,0)
            AND price.statecode = cust.statecode
            AND price.MrpPerPack = idt.mrp 
            AND price.startdate<=idt.idtmngdate
            WHEN idt.mrp = '0' THEN LTRIM (idt.prdcode,0) = LTRIM (price.productcode,0)
            AND price.statecode = cust.statecode
            AND (LTRIM (idt.prdcode,0) ||cust.statecode||price.MrpPerPack) IN (SELECT (LTRIM(productcode,0) ||statecode||MrpPerPack)
                                                                                    FROM (SELECT DISTINCT productcode,
                                                                                                statecode,
                                                                                                startDate,
                                                                                                insertedOn,
                                                                                                MrpPerPack,
                                                                                                ROW_NUMBER() OVER (PARTITION BY productcode,statecode ORDER BY startDate DESC,insertedOn DESC,MrpPerPack) AS rk
                                                                                            FROM itg_pricelist)
                                                                                    WHERE rk = 1) END = 1
        INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                            CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                            caldate
                    FROM edw_retailer_calendar_dim
                    WHERE mth_mm IN (SELECT (year||lpad (MONTH,2,0))
                                    FROM itg_mds_month_end_dates
                                    WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp())) <= to_date(pathfinder_month_end)
                                                )) cal_dim ON to_date (idt.idtmngdate) = to_date (cal_dim.caldate)) a
)
select serno::varchar(50) as serno,
    distcode::varchar(50) as distcode,
    prdcode::varchar(50) as prdcode,
    idtmngdate::timestamp_ntz(9) as idtmngdate,
    stkmgmttype::varchar(30) as stkmgmttype,
    status::varchar(100) as status,
    stocktype::varchar(50) as stocktype,
    baseqty::number(18,0) as baseqty,
    mrp::number(18,2) as mrp,
    listprice::number(18,2) as listprice,
    nr::number(18,2) as nr,
    ptr::number(18,2) as ptr,
    month::varchar(50) as month,
    year::varchar(50) as year,
    src::varchar(10) as src,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from final