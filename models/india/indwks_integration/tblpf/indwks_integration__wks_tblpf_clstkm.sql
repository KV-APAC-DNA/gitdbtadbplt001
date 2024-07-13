with itg_mds_month_end_dates as
(
    select * from {{ ref('inditg_integration__itg_mds_month_end_dates') }}
),
itg_rmrpstockprocess_clstk as
(
    select * from {{ ref('inditg_integration__itg_rmrpstockprocess_clstk') }}
),
itg_rmrpstockprocess_opstk as 
(
    select * from {{ ref('inditg_integration__itg_rmrpstockprocess_opstk') }}
),
edw_retailer_calendar_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
edw_customer_dim as 
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
mon_end as
(SELECT YEAR,
                                         MONTH,
                                         pathfinder_month_end
                                  FROM itg_mds_month_end_dates),
cal_dim as
(SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                         CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                         caldate
                                  FROM edw_retailer_calendar_dim
                                  WHERE mth_mm IN (SELECT (year||lpad (MONTH,2,0))
                                                   FROM itg_mds_month_end_dates
                                                   WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp()::timestamp_ntz(9))) <= to_date(pathfinder_month_end)
                                                   )),
cls1 as 
(SELECT DISTINCT distcode,
                                 transdate,
                                 createddate,
                                 productcode AS prdcode,
                                 SUM(salclsstock*lsp) OVER (PARTITION BY distcode,productcode,transdate) / NULLIF(SUM(salclsstock) OVER (PARTITION BY distcode,productcode,transdate),0) AS lp,
                                 SUM(salclsstock*selrate) OVER (PARTITION BY distcode,productcode,transdate) / NULLIF(SUM(salclsstock) OVER (PARTITION BY distcode,productcode,transdate),0) AS ptr,
                                 SUM(salclsstock*nrvalue) OVER (PARTITION BY distcode,productcode,transdate) / NULLIF(SUM(salclsstock) OVER (PARTITION BY distcode,productcode,transdate),0) AS nr,
                                 SUM(lsp*salclsstock) OVER (PARTITION BY distcode,productcode,transdate) AS lpvalue,
                                 SUM(selrate*salclsstock) OVER (PARTITION BY distcode,productcode,transdate) AS ptrvalue,
                                 SUM(nrvalue*salclsstock) OVER (PARTITION BY distcode,productcode,transdate) AS value,
                                 SUM(salclsstock*nrvalue) OVER (PARTITION BY distcode,productcode,transdate) AS salclsnrvalue,
                                 SUM(unsalclsstock*nrvalue) OVER (PARTITION BY distcode,productcode,transdate) AS unsalclsnrvalue,
                                 SUM(offerclsstock*nrvalue) OVER (PARTITION BY distcode,productcode,transdate) AS offerclsnrvalue,
                                 SUM(salclsstock) OVER (PARTITION BY distcode,productcode,transdate) AS salclsstock,
                                 SUM(unsalclsstock) OVER (PARTITION BY distcode,productcode,transdate) AS unsalclsstock,
                                 SUM(offerclsstock) OVER (PARTITION BY distcode,productcode,transdate) AS offerclsstock
                          FROM itg_rmrpstockprocess_clstk),
cls as 
(SELECT cls1.distcode,
                           cls1.transdate,
                           cls1.createddate,
                           cls1.prdcode,
                           cls1.lp,
                           cls1.ptr,
                           cls1.nr,
                           cls1.lpvalue,
                           cls1.ptrvalue,
                           cls1.value,
                           cls1.salclsnrvalue,
                           cls1.unsalclsnrvalue,
                           cls1.offerclsnrvalue,
                           cls1.salclsstock,
                           cls1.unsalclsstock,
                           cls1.offerclsstock,
                           cal_dim.mon,
                           cal_dim.yr,
                           cal_dim.caldate,
                           mon_end.YEAR,
                           mon_end.MONTH,
                           mon_end.pathfinder_month_end
                    FROM cls1
                      INNER JOIN cal_dim ON cls1.transdate = cal_dim.caldate
                      INNER JOIN mon_end
                              ON mon_end.year = cal_dim.yr
                             AND mon_end.month = cal_dim.mon
                    WHERE to_date(createddate) <= to_date(pathfinder_month_end)),
opn as
(SELECT opn1.distcode,
                          opn1.transdate,
                          opn1.productcode,
                          opn1.salopenstock,
                          opn1.unsalopenstock,
                          opn1.offeropenstock,
                          cal_dim_opn.mon,
                          cal_dim_opn.yr,
                          cal_dim_opn.caldate
                   FROM (SELECT DISTINCT distcode,
                                transdate,
                                productcode,
                                SUM(salopenstock) OVER (PARTITION BY distcode,productcode,transdate) AS salopenstock,
                                SUM(unsalopenstock) OVER (PARTITION BY distcode,productcode,transdate) AS unsalopenstock,
                                SUM(offeropenstock) OVER (PARTITION BY distcode,productcode,transdate) AS offeropenstock
                         FROM itg_rmrpstockprocess_opstk) opn1
                     INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                        CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                        caldate
                                 FROM edw_retailer_calendar_dim
                                 WHERE mth_mm IN (SELECT (year||lpad (MONTH,2,0))
                                                  FROM itg_mds_month_end_dates
                                                  WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp()::timestamp_ntz(9))) <= to_date(pathfinder_month_end)
                                                  )) cal_dim_opn ON opn1.transdate = cal_dim_opn.caldate),
cte as 
(SELECT CAST(NULL AS BIGINT) AS serno,
             cls.mon,
             cls.yr,
             cls.prdcode,
             cls.distcode,
             ifnull(SUM(salopenstock) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr),0) AS salopenstock,
             ifnull(SUM(unsalopenstock) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr),0) AS unsalopenstock,
             ifnull(SUM(offeropenstock) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr),0) AS offeropenstock,
             SUM(unsalclsstock) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr) AS unsalclsstock,
             SUM(offerclsstock) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr) AS offerclsstock,
             SUM(salclsstock) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr) AS clstckqty,
             SUM(cls.lp) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr) AS lp,
             SUM(cls.ptr) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr) AS ptr,
             SUM(cls.nr) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr) AS nr,
             SUM(cls.lpvalue) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr) AS lpvalue,
             SUM(cls.ptrvalue) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr) AS ptrvalue,
             SUM(cls.value) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr) AS value,
             'N' AS iscubeprocess,
             SUM(cls.salclsnrvalue) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr) AS salclsnrvalue,
             SUM(cls.unsalclsnrvalue) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr) AS unsalclsnrvalue,
             SUM(cls.offerclsnrvalue) OVER (PARTITION BY cls.distcode,cls.prdcode,cls.mon,cls.yr) AS offerclsnrvalue
      FROM (SELECT DISTINCT customer_code
            FROM edw_customer_dim
            WHERE UPPER(psnonps) = 'Y') cust
        INNER JOIN cls ON cls.distcode = cust.customer_code
        LEFT JOIN opn
               ON cls.distcode = opn.distcode
              AND cls.prdcode = opn.productcode
              AND cls.mon = opn.mon
              AND cls.yr = opn.yr),
transformed as 
(
SELECT DISTINCT serno,
       mon,
       yr,
       prdcode,
       distcode,
       salopenstock,
       unsalopenstock,
       offeropenstock,
       unsalclsstock,
       offerclsstock,
       clstckqty,
       lp,
       ptr,
       nr,
       'PWS' AS src,
       value,
       lpvalue,
       ptrvalue,
       iscubeprocess,
       salclsnrvalue,
       unsalclsnrvalue,
       offerclsnrvalue,
       convert_timezone('Asia/Kolkata',current_timestamp()::timestamp_ntz(9)) AS crt_dttm,
       convert_timezone('Asia/Kolkata',current_timestamp()::timestamp_ntz(9)) AS updt_dttm
FROM cte
),
final as 
(
    select
        serno::number(38,0) as serno,
        mon::number(18,0) as mon,
        yr::number(18,0) as yr,
        prdcode::varchar(50) as prdcode,
        distcode::varchar(50) as distcode,
        COALESCE(salopenstock,0)::number(18,3) as salopenstock,
        COALESCE(unsalopenstock,0)::number(18,3) as unsalopenstock,
        COALESCE(offeropenstock,0)::number(18,3) as offeropenstock,
        COALESCE(unsalclsstock,0)::number(18,3) as unsalclsstock,
        COALESCE(offerclsstock,0)::number(18,3) as offerclsstock,
        COALESCE(clstckqty,0)::number(18,3) as clstckqty,
        COALESCE(lp,0)::number(18,3) as lp,
        COALESCE(ptr,0)::number(18,3) as ptr,
        COALESCE(nr,0)::number(18,3) as nr,
        src::varchar(10) as src,
        COALESCE(value,0)::number(18,3) as value,
        COALESCE(lpvalue,0)::number(18,3) as lpvalue,
        COALESCE(ptrvalue,0)::number(18,3) as ptrvalue,
        iscubeprocess::varchar(2) as iscubeprocess,
        COALESCE(salclsnrvalue,0)::number(18,3) as salclsnrvalue,
        COALESCE(unsalclsnrvalue,0)::number(18,3) as unsalclsnrvalue,
        COALESCE(offerclsnrvalue,0)::number(18,3) as offerclsnrvalue,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final