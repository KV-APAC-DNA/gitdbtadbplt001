with 
itg_dailysales as 
(
    select * from {{ ref('inditg_integration__itg_dailysales') }}
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
itg_dailysales_dbrestore as 
(
    select * from {{ ref('inditg_integration__itg_dailysales_dbrestore') }}
),
itg_salesreturn as 
(
    select * from {{ ref('inditg_integration__itg_salesreturn') }}
),
itg_salesreturn_dbrestore as 
(
    select * from {{ ref('inditg_integration__itg_salesreturn_dbrestore') }}
),
itg_pricelist as 
(
    select * from {{ ref('inditg_integration__itg_pricelist') }}
),
edw_billing_fact as 
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
trans as 
(
    SELECT 
       ds.serno,
       ds.mon,
       ds.yr,
       ds.distcode,
       ds.prdcode,
       ds.nr,
       ds.prdqty,
       ds.prdgrossamt,
       ds.ptrvalue,
       ds.src,
       ds.prdnrvalue,
       ds.lpvalue,
       ds.trinqty,
       ds.troutqty,
       ds.trinval,
       ds.troutval,
       ds.nonwaveopenqty,
       ds.runmm,
       ds.runyr,
       ds.iscubeprocess,
       ds.pricelistlp,
       ds.pricelistptr,
       ds.prdqty_new,
       ds.prdnrvalue_new,
       ds.ptrvalue_new,
       ds.dbrestore_nr_prev,
       ds.dbrestore_prdqty_prev,
       ds.dbrestore_prdgrossamt_prev,
       ds.dbrestore_prdnrvalue_prev,
       ds.dbrestore_ptrvalue_prev,
       ds.dbrestore_nr_curr,
       ds.dbrestore_prdqty_curr,
       ds.dbrestore_prdgrossamt_curr,
       ds.dbrestore_prdnrvalue_curr,
       ds.dbrestore_ptrvalue_curr,
       convert_timezone('Asia/Kolkata',current_timestamp()) AS crt_dttm,
       convert_timezone('Asia/Kolkata',current_timestamp()) AS updt_dttm
FROM 
(
       SELECT a1.serno,
             a1.mon,
             a1.yr,
             a1.distcode,
             a1.prdcode,
             a1.nr,
             SUM(a1.prdqty) AS prdqty,
             SUM(a1.prdgrossamt) AS prdgrossamt,
             SUM(a1.ptrvalue) AS ptrvalue,
             a1.src,
             SUM(a1.prdnrvalue) AS prdnrvalue,
             SUM(a1.lpvalue) AS lpvalue,
             SUM(a1.trinqty) AS trinqty,
             SUM(a1.troutqty) AS troutqty,
             SUM(a1.trinval) AS trinval,
             SUM(a1.troutval) AS troutval,
             SUM(a1.nonwaveopenqty) AS nonwaveopenqty,
             a1.runmm,
             a1.runyr,
             a1.iscubeprocess,
             SUM(a1.pricelistlp) AS pricelistlp,
             SUM(a1.pricelistptr) AS pricelistptr,
             SUM(a1.prdqty_new) AS prdqty_new,
             SUM(a1.prdnrvalue_new) AS prdnrvalue_new,
             SUM(a1.ptrvalue_new) AS ptrvalue_new,
             a1.dbrestore_nr_prev,
             SUM(a1.dbrestore_prdqty_prev) AS dbrestore_prdqty_prev,
             SUM(a1.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_prev,
             SUM(a1.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_prev,
             SUM(a1.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_prev,
             a1.dbrestore_nr_curr,
             SUM(a1.dbrestore_prdqty_curr) AS dbrestore_prdqty_curr,
             SUM(a1.dbrestore_prdgrossamt_curr) AS dbrestore_prdgrossamt_curr,
             SUM(a1.dbrestore_prdnrvalue_curr) AS dbrestore_prdnrvalue_curr,
             SUM(a1.dbrestore_ptrvalue_curr) AS dbrestore_ptrvalue_curr
      FROM (SELECT DISTINCT CAST(NULL AS BIGINT) AS serno,
                   cal_dim.mon,
                   cal_dim.yr,
                   dscurr.distcode,
                   dscurr.prdcode,
                   dscurr.nr,
                   dscurr.mrp,
                   dscurr.salinvdate,
                   SUM(dscurr.prdqty) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS prdqty,
                   SUM(dscurr.prdgrossamt) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS prdgrossamt,
                   SUM(dscurr.ptrvalue) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS ptrvalue,
                   dscurr.src,
                   SUM(dscurr.ab) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS prdnrvalue,
                   0 AS lpvalue,
                   0 AS trinqty,
                   0 AS troutqty,
                   0 AS trinval,
                   0 AS troutval,
                   CAST(NULL AS INT) AS nonwaveopenqty,
                   cal_dim.mon AS runmm,
                   cal_dim.yr AS runyr,
                   CAST(NULL AS INT) AS iscubeprocess,
                   0 AS pricelistlp,
                   0 AS pricelistptr,
                   SUM(dscurr.prdqty) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS prdqty_new,
                   SUM(dscurr.ab) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS prdnrvalue_new,
                   SUM(dscurr.ptrvalue) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS ptrvalue_new,
                   0 AS dbrestore_nr_prev,
                   0 AS dbrestore_prdqty_prev,
                   0 AS dbrestore_prdgrossamt_prev,
                   0 AS dbrestore_prdnrvalue_prev,
                   0 AS dbrestore_ptrvalue_prev,
                   0 AS dbrestore_nr_curr,
                   0 AS dbrestore_prdqty_curr,
                   0 AS dbrestore_prdgrossamt_curr,
                   0 AS dbrestore_prdnrvalue_curr,
                   0 AS dbrestore_ptrvalue_curr
            FROM (SELECT distcode AS distcode,
                         prdcode AS prdcode,
                         (nrvalue) AS nr,
                         prdqty,
                         nrvalue*prdqty AS ab,
                         (prdqty*prdselrateaftertax) AS ptrvalue,
                         (prdgrossamt),
                         'DS' AS src,
                         mrp,
                         salinvdate,
                         modifieddate
                  FROM (SELECT distcode,
                               prdcode,
                               nrvalue,
                               prdqty,
                               prdselrateaftertax,
                               prdgrossamt,
                               mrp,
                               salinvdate,
                               modifieddate
                        FROM itg_dailysales)) dscurr
              INNER JOIN (SELECT DISTINCT customer_code
                          FROM edw_customer_dim
                          WHERE UPPER(psnonps) = 'Y'
                          AND   (UPPER(active_flag) = 'Y' or UPPER(active_flag) = 'N')
                          AND   customer_code <> '') cust ON LTRIM (dscurr.distcode,'0') = cust.customer_code
              INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                 CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                 caldate
                          FROM edw_retailer_calendar_dim
                          WHERE mth_mm IN (SELECT (year||lpad (MONTH,2,0))
                                           FROM itg_mds_month_end_dates
                                           WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp())) <= to_date(pathfinder_month_end))) cal_dim ON to_date (dscurr.salinvdate) = to_date (cal_dim.caldate)
              INNER JOIN (SELECT YEAR,
                                 MONTH,
                                 pathfinder_month_end
                          FROM itg_mds_month_end_dates) mon_end
                      ON mon_end.year = cal_dim.yr
                     AND mon_end.month = cal_dim.mon
            WHERE to_date(modifieddate) <= to_date(pathfinder_month_end)) a1
      GROUP BY mon,
               yr,
               distcode,
               prdcode,
               nr,
               src,
               runmm,
               runyr,
               iscubeprocess,
               serno,
               dbrestore_nr_prev,
               dbrestore_nr_curr
      UNION ALL
      SELECT a1runmm.serno,
             a1runmm.mon,
             a1runmm.yr,
             a1runmm.distcode,
             a1runmm.prdcode,
             a1runmm.nr,
             SUM(a1runmm.prdqty) AS prdqty,
             SUM(a1runmm.prdgrossamt) AS prdgrossamt,
             SUM(a1runmm.ptrvalue) AS ptrvalue,
             a1runmm.src,
             SUM(a1runmm.prdnrvalue) AS prdnrvalue,
             SUM(a1runmm.lpvalue) AS lpvalue,
             SUM(a1runmm.trinqty) AS trinqty,
             SUM(a1runmm.troutqty) AS troutqty,
             SUM(a1runmm.trinval) AS trinval,
             SUM(a1runmm.troutval) AS troutval,
             SUM(a1runmm.nonwaveopenqty) AS nonwaveopenqty,
             a1runmm.runmm,
             a1runmm.runyr,
             a1runmm.iscubeprocess,
             SUM(a1runmm.pricelistlp) AS pricelistlp,
             SUM(a1runmm.pricelistptr) AS pricelistptr,
             SUM(a1runmm.prdqty_new) AS prdqty_new,
             SUM(a1runmm.prdnrvalue_new) AS prdnrvalue_new,
             SUM(a1runmm.ptrvalue_new) AS ptrvalue_new,
             a1runmm.dbrestore_nr_prev,
             SUM(a1runmm.dbrestore_prdqty_prev) AS dbrestore_prdqty_prev,
             SUM(a1runmm.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_prev,
             SUM(a1runmm.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_prev,
             SUM(a1runmm.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_prev,
             a1runmm.dbrestore_nr_curr,
             SUM(a1runmm.dbrestore_prdqty_curr) AS dbrestore_prdqty_curr,
             SUM(a1runmm.dbrestore_prdgrossamt_curr) AS dbrestore_prdgrossamt_curr,
             SUM(a1runmm.dbrestore_prdnrvalue_curr) AS dbrestore_prdnrvalue_curr,
             SUM(a1runmm.dbrestore_ptrvalue_curr) AS dbrestore_ptrvalue_curr
      FROM (SELECT DISTINCT CAST(NULL AS BIGINT) AS serno,
                   cal_dim.mon,
                   cal_dim.yr,
                   dscurr.distcode,
                   dscurr.prdcode,
                   dscurr.nr,
                   dscurr.mrp,
                   dscurr.salinvdate,
                   SUM(dscurr.prdqty) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS prdqty,
                   SUM(dscurr.prdgrossamt) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS prdgrossamt,
                   SUM(dscurr.ptrvalue) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS ptrvalue,
                   dscurr.src,
                   SUM(dscurr.ab) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS prdnrvalue,
                   0 AS lpvalue,
                   0 AS trinqty,
                   0 AS troutqty,
                   0 AS trinval,
                   0 AS troutval,
                   CAST(NULL AS INT) AS nonwaveopenqty,
                   CAST(SUBSTRING(to_date(cal_dim.run_yearmonth),6,2) AS INT) AS runmm,
                   CAST(SUBSTRING(to_date(cal_dim.run_yearmonth),1,4) AS INT) AS runyr,
                   CAST(NULL AS INT) AS iscubeprocess,
                   0 AS pricelistlp,
                   0 AS pricelistptr,
                   SUM(dscurr.prdqty) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS prdqty_new,
                   SUM(dscurr.ab) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS prdnrvalue_new,
                   SUM(dscurr.ptrvalue) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS ptrvalue_new,
                   0 AS dbrestore_nr_prev,
                   0 AS dbrestore_prdqty_prev,
                   0 AS dbrestore_prdgrossamt_prev,
                   0 AS dbrestore_prdnrvalue_prev,
                   0 AS dbrestore_ptrvalue_prev,
                   0 AS dbrestore_nr_curr,
                   0 AS dbrestore_prdqty_curr,
                   0 AS dbrestore_prdgrossamt_curr,
                   0 AS dbrestore_prdnrvalue_curr,
                   0 AS dbrestore_ptrvalue_curr
            FROM (SELECT distcode AS distcode,
                         prdcode AS prdcode,
                         (nrvalue) AS nr,
                         prdqty,
                         nrvalue*prdqty AS ab,
                         prdselrateaftertax,
                         (prdqty*prdselrateaftertax) AS ptrvalue,
                         (prdgrossamt),
                         'DS' AS src,
                         mrp,
                         salinvdate,
                         modifieddate
                  FROM (SELECT distcode,
                               prdcode,
                               nrvalue,
                               prdqty,
                               prdselrateaftertax,
                               prdgrossamt,
                               mrp,
                               salinvdate,
                               modifieddate
                        FROM itg_dailysales)) dscurr
              INNER JOIN (SELECT DISTINCT customer_code
                          FROM edw_customer_dim
                          WHERE UPPER(psnonps) = 'Y'
                          AND   (UPPER(active_flag) = 'Y' or UPPER(active_flag) = 'N')
                          AND   customer_code <> '') cust ON LTRIM (dscurr.distcode,'0') = cust.customer_code
              INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                 CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                 add_months(TO_DATE(SUBSTRING(mth_mm,1,4) ||SUBSTRING (mth_mm,5,2),'YYYYMM'),1) AS run_yearmonth,
                                 caldate
                          FROM edw_retailer_calendar_dim
                          WHERE mth_mm IN (SELECT TO_CHAR(add_months (TO_DATE(concat (YEAR,LPAD(MONTH,2,0)),'YYYYMM'),-1),'YYYYMM')
                                           FROM itg_mds_month_end_dates
                                           WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp())) <= to_date(pathfinder_month_end))) cal_dim ON to_date (dscurr.salinvdate) = to_date (cal_dim.caldate)
              INNER JOIN (SELECT YEAR,
                                 MONTH,
                                 pathfinder_month_end
                          FROM itg_mds_month_end_dates) mon_end
                      ON mon_end.year = cal_dim.yr
                     AND mon_end.month = cal_dim.mon
            WHERE to_date(modifieddate) > to_date(pathfinder_month_end)) a1runmm
      GROUP BY mon,
               yr,
               distcode,
               prdcode,
               nr,
               src,
               runmm,
               runyr,
               iscubeprocess,
               serno,
               dbrestore_nr_prev,
               dbrestore_nr_curr
      UNION ALL
      SELECT a1dbres.serno,
             a1dbres.mon,
             a1dbres.yr,
             a1dbres.distcode,
             a1dbres.prdcode,
             a1dbres.nr,
             SUM(a1dbres.prdqty) AS prdqty,
             SUM(a1dbres.prdgrossamt) AS prdgrossamt,
             SUM(a1dbres.ptrvalue) AS ptrvalue,
             a1dbres.src,
             SUM(a1dbres.prdnrvalue) AS prdnrvalue,
             SUM(a1dbres.lpvalue) AS lpvalue,
             SUM(a1dbres.trinqty) AS trinqty,
             SUM(a1dbres.troutqty) AS troutqty,
             SUM(a1dbres.trinval) AS trinval,
             SUM(a1dbres.troutval) AS troutval,
             SUM(a1dbres.nonwaveopenqty) AS nonwaveopenqty,
             a1dbres.runmm,
             a1dbres.runyr,
             a1dbres.iscubeprocess,
             SUM(a1dbres.pricelistlp) AS pricelistlp,
             SUM(a1dbres.pricelistptr) AS pricelistptr,
             SUM(a1dbres.prdqty_new) AS prdqty_new,
             SUM(a1dbres.prdnrvalue_new) AS prdnrvalue_new,
             SUM(a1dbres.ptrvalue_new) AS ptrvalue_new,
             a1dbres.dbrestore_nr_prev,
             SUM(a1dbres.dbrestore_prdqty_prev) AS dbrestore_prdqty_prev,
             SUM(a1dbres.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_prev,
             SUM(a1dbres.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_prev,
             SUM(a1dbres.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_prev,
             a1dbres.dbrestore_nr_curr,
             SUM(a1dbres.dbrestore_prdqty_curr) AS dbrestore_prdqty_curr,
             SUM(a1dbres.dbrestore_prdgrossamt_curr) AS dbrestore_prdgrossamt_curr,
             SUM(a1dbres.dbrestore_prdnrvalue_curr) AS dbrestore_prdnrvalue_curr,
             SUM(a1dbres.dbrestore_ptrvalue_curr) AS dbrestore_ptrvalue_curr
      FROM (SELECT DISTINCT CAST(NULL AS BIGINT) AS serno,
                   cal_dim.mon,
                   cal_dim.yr,
                   dscurr.distcode,
                   dscurr.prdcode,
                   0 AS nr,
                   0 AS mrp,
                   dscurr.salinvdate,
                   0 AS prdqty,
                   0 AS prdgrossamt,
                   0 AS ptrvalue,
                   dscurr.src,
                   0 AS prdnrvalue,
                   0 AS lpvalue,
                   0 AS trinqty,
                   0 AS troutqty,
                   0 AS trinval,
                   0 AS troutval,
                   CAST(NULL AS INT) AS nonwaveopenqty,
                   CAST(SUBSTRING(to_date(cal_dim.run_yearmonth),6,2) AS INT) AS runmm,
                   CAST(SUBSTRING(to_date(cal_dim.run_yearmonth),1,4) AS INT) AS runyr,
                   CAST(NULL AS INT) AS iscubeprocess,
                   0 AS pricelistlp,
                   0 AS pricelistptr,
                   0 AS prdqty_new,
                   0 AS prdnrvalue_new,
                   0 AS ptrvalue_new,
                   dscurr.nr AS dbrestore_nr_prev,
                   SUM(dscurr.prdqty) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS dbrestore_prdqty_prev,
                   SUM(dscurr.prdgrossamt) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS dbrestore_prdgrossamt_prev,
                   SUM(dscurr.ab) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS dbrestore_prdnrvalue_prev,
                   SUM(dscurr.ptrvalue) OVER (PARTITION BY dscurr.distcode,dscurr.prdcode,dscurr.salinvdate,dscurr.mrp,dscurr.nr) AS dbrestore_ptrvalue_prev,
                   0 AS dbrestore_nr_curr,
                   0 AS dbrestore_prdqty_curr,
                   0 AS dbrestore_prdgrossamt_curr,
                   0 AS dbrestore_prdnrvalue_curr,
                   0 AS dbrestore_ptrvalue_curr
            FROM (SELECT distcode AS distcode,
                         prdcode AS prdcode,
                         (nrvalue) AS nr,
                         prdqty,
                         nrvalue*prdqty AS ab,
                         prdselrateaftertax,
                         (prdqty*prdselrateaftertax) AS ptrvalue,
                         (prdgrossamt),
                         'DS' AS src,
                         mrp,
                         salinvdate,
                         createddate,
                         createddt
                  FROM (SELECT distcode,
                               prdcode,
                               nrvalue,
                               prdqty,
                               prdselrateaftertax,
                               prdgrossamt,
                               mrp,
                               salinvdate,
                               createddate,
                               createddt
                        FROM itg_dailysales_dbrestore)) dscurr
              INNER JOIN (SELECT DISTINCT customer_code
                          FROM edw_customer_dim
                          WHERE UPPER(psnonps) = 'Y'
                          AND   (UPPER(active_flag) = 'Y' or UPPER(active_flag) = 'N')
                          AND   customer_code <> '') cust ON LTRIM (dscurr.distcode,'0') = cust.customer_code
              INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                 CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                 add_months(TO_DATE(SUBSTRING(mth_mm,1,4) ||SUBSTRING (mth_mm,5,2),'YYYYMM'),1) AS run_yearmonth,
                                 caldate
                          FROM edw_retailer_calendar_dim
                          WHERE mth_mm IN (SELECT TO_CHAR(add_months (TO_DATE(concat (YEAR,LPAD(MONTH,2,0)),'YYYYMM'),-1),'YYYYMM')
                                           FROM itg_mds_month_end_dates
                                           WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp())) <= to_date(pathfinder_month_end))) cal_dim ON to_date (dscurr.salinvdate) = to_date (cal_dim.caldate)
              INNER JOIN (SELECT YEAR,
                                 MONTH,
                                 pathfinder_month_end
                          FROM itg_mds_month_end_dates) mon_end
                      ON mon_end.year = cal_dim.yr
                     AND mon_end.month = cal_dim.mon
            WHERE to_date(createddt) > to_date(pathfinder_month_end)) a1dbres
      GROUP BY mon,
               yr,
               distcode,
               prdcode,
               nr,
               src,
               runmm,
               runyr,
               iscubeprocess,
               serno,
               dbrestore_nr_prev,
               dbrestore_nr_curr
      UNION ALL
      SELECT a1dbrcur.serno,
             a1dbrcur.mon,
             a1dbrcur.yr,
             a1dbrcur.distcode,
             a1dbrcur.prdcode,
             a1dbrcur.nr,
             SUM(a1dbrcur.prdqty) AS prdqty,
             SUM(a1dbrcur.prdgrossamt) AS prdgrossamt,
             SUM(a1dbrcur.ptrvalue) AS ptrvalue,
             a1dbrcur.src,
             SUM(a1dbrcur.prdnrvalue) AS prdnrvalue,
             SUM(a1dbrcur.lpvalue) AS lpvalue,
             SUM(a1dbrcur.trinqty) AS trinqty,
             SUM(a1dbrcur.troutqty) AS troutqty,
             SUM(a1dbrcur.trinval) AS trinval,
             SUM(a1dbrcur.troutval) AS troutval,
             SUM(a1dbrcur.nonwaveopenqty) AS nonwaveopenqty,
             a1dbrcur.runmm,
             a1dbrcur.runyr,
             a1dbrcur.iscubeprocess,
             SUM(a1dbrcur.pricelistlp) AS pricelistlp,
             SUM(a1dbrcur.pricelistptr) AS pricelistptr,
             SUM(a1dbrcur.prdqty_new) AS prdqty_new,
             SUM(a1dbrcur.prdnrvalue_new) AS prdnrvalue_new,
             SUM(a1dbrcur.ptrvalue_new) AS ptrvalue_new,
             a1dbrcur.dbrestore_nr_prev,
             SUM(a1dbrcur.dbrestore_prdqty_prev) AS dbrestore_prdqty_prev,
             SUM(a1dbrcur.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_prev,
             SUM(a1dbrcur.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_prev,
             SUM(a1dbrcur.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_prev,
             a1dbrcur.dbrestore_nr_curr,
             SUM(a1dbrcur.dbrestore_prdqty_curr) AS dbrestore_prdqty_curr,
             SUM(a1dbrcur.dbrestore_prdgrossamt_curr) AS dbrestore_prdgrossamt_curr,
             SUM(a1dbrcur.dbrestore_prdnrvalue_curr) AS dbrestore_prdnrvalue_curr,
             SUM(a1dbrcur.dbrestore_ptrvalue_curr) AS dbrestore_ptrvalue_curr
      FROM (SELECT DISTINCT CAST(NULL AS BIGINT) AS serno,
                   dbrcurr.mon,
                   dbrcurr.yr,
                   dbrcurr.distcode,
                   dbrcurr.prdcode,
                   0 AS nr,
                   0 AS mrp,
                   dbrcurr.salinvdate,
                   0 AS prdqty,
                   0 AS prdgrossamt,
                   0 AS ptrvalue,
                   dbrcurr.src,
                   0 AS prdnrvalue,
                   0 AS lpvalue,
                   0 AS trinqty,
                   0 AS troutqty,
                   0 AS trinval,
                   0 AS troutval,
                   CAST(NULL AS INT) AS nonwaveopenqty,
                   CAST(SUBSTRING(to_date(dbrcurr.run_yearmonth),6,2) AS INT) AS runmm,
                   CAST(SUBSTRING(to_date(dbrcurr.run_yearmonth),1,4) AS INT) AS runyr,
                   CAST(NULL AS INT) AS iscubeprocess,
                   0 AS pricelistlp,
                   0 AS pricelistptr,
                   0 AS prdqty_new,
                   0 AS prdnrvalue_new,
                   0 AS ptrvalue_new,
                   0 AS dbrestore_nr_prev,
                   0 AS dbrestore_prdqty_prev,
                   0 AS dbrestore_prdgrossamt_prev,
                   0 AS dbrestore_prdnrvalue_prev,
                   0 AS dbrestore_ptrvalue_prev,
                   dbrcurr.nr AS dbrestore_nr_curr,
                   SUM(dbrcurr.prdqty) OVER (PARTITION BY dbrcurr.distcode,dbrcurr.prdcode,dbrcurr.salinvdate,dbrcurr.mrp,dbrcurr.nr) AS dbrestore_prdqty_curr,
                   SUM(dbrcurr.prdgrossamt) OVER (PARTITION BY dbrcurr.distcode,dbrcurr.prdcode,dbrcurr.salinvdate,dbrcurr.mrp,dbrcurr.nr) AS dbrestore_prdgrossamt_curr,
                   SUM(dbrcurr.ab) OVER (PARTITION BY dbrcurr.distcode,dbrcurr.prdcode,dbrcurr.salinvdate,dbrcurr.mrp,dbrcurr.nr) AS dbrestore_prdnrvalue_curr,
                   SUM(dbrcurr.ptrvalue) OVER (PARTITION BY dbrcurr.distcode,dbrcurr.prdcode,dbrcurr.salinvdate,dbrcurr.mrp,dbrcurr.nr) AS dbrestore_ptrvalue_curr
            FROM (SELECT main.distcode,
                         main.prdcode,
                         (main.nrvalue) AS nr,
                         main.prdqty,
                         (main.nrvalue*main.prdqty) AS ab,
                         main.prdselrateaftertax,
                         (main.prdqty*main.prdselrateaftertax) AS ptrvalue,
                         main.prdgrossamt,
                         'DS' AS src,
                         main.mrp,
                         main.salinvdate,
                         main.modifieddate,
                         main.mon,
                         main.yr,
                         main.run_yearmonth
                  FROM (SELECT dscurr.distcode,
                               dscurr.prdcode,
                               dscurr.nrvalue,
                               dscurr.prdqty,
                               dscurr.prdselrateaftertax,
                               dscurr.prdgrossamt,
                               dscurr.mrp,
                               dscurr.salinvdate,
                               dscurr.salinvno,
                               dscurr.modifieddate,
                               cal_dim.mon,
                               cal_dim.yr,
                               cal_dim.run_yearmonth
                        FROM itg_dailysales dscurr
                          INNER JOIN (SELECT DISTINCT customer_code
                                      FROM edw_customer_dim
                                      WHERE UPPER(psnonps) = 'Y'
                                      AND   (UPPER(active_flag) = 'Y' or UPPER(active_flag) = 'N')
                                      AND   customer_code <> '') cust ON LTRIM (dscurr.distcode,'0') = cust.customer_code
                          INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                             CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                             add_months(TO_DATE(SUBSTRING(mth_mm,1,4) ||SUBSTRING (mth_mm,5,2),'YYYYMM'),1) AS run_yearmonth,
                                             caldate
                                      FROM edw_retailer_calendar_dim
                                      WHERE mth_mm IN (SELECT TO_CHAR(add_months (TO_DATE(concat (YEAR,LPAD(MONTH,2,0)),'YYYYMM'),-1),'YYYYMM')
                                                       FROM itg_mds_month_end_dates
                                                       WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp())) <= to_date(pathfinder_month_end))) cal_dim ON to_date (dscurr.salinvdate) = to_date (cal_dim.caldate)
                          INNER JOIN (SELECT YEAR,
                                             MONTH,
                                             pathfinder_month_end
                                      FROM itg_mds_month_end_dates) mon_end
                                  ON mon_end.year = cal_dim.yr
                                 AND mon_end.month = cal_dim.mon
                        WHERE to_date(modifieddate) > to_date(pathfinder_month_end)
                        AND   EXISTS (SELECT 1
                                      FROM (SELECT DISTINCT distcode,
                                                   salinvno
                                            FROM itg_dailysales_dbrestore dbs
                                              INNER JOIN (SELECT DISTINCT customer_code
                                                          FROM edw_customer_dim
                                                          WHERE UPPER(psnonps) = 'Y'
                                                          AND   (UPPER(active_flag) = 'Y' or UPPER(active_flag) = 'N')
                                                          AND   customer_code <> '') cust ON LTRIM (dbs.distcode,'0') = cust.customer_code
                                              INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                                                 CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                                                 add_months(TO_DATE(SUBSTRING(mth_mm,1,4) ||SUBSTRING (mth_mm,5,2),'YYYYMM'),1) AS run_yearmonth,
                                                                 caldate
                                                          FROM edw_retailer_calendar_dim
                                                          WHERE mth_mm IN (SELECT TO_CHAR(add_months (TO_DATE(concat (YEAR,LPAD(MONTH,2,0)),'YYYYMM'),-1),'YYYYMM')
                                                                           FROM itg_mds_month_end_dates
                                                                           WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp())) <= to_date(pathfinder_month_end))) cal_dim ON to_date (dbs.salinvdate) = to_date (cal_dim.caldate)
                                              INNER JOIN (SELECT YEAR,
                                                                 MONTH,
                                                                 pathfinder_month_end
                                                          FROM itg_mds_month_end_dates) mon_end
                                                      ON mon_end.year = cal_dim.yr
                                                     AND mon_end.month = cal_dim.mon
                                            WHERE to_date(createddt) > to_date(pathfinder_month_end)) dbsref
                                      WHERE COALESCE(dscurr.distcode,'NA') = COALESCE(dbsref.distcode,'NA')
                                      AND   COALESCE(UPPER(dscurr.salinvno),'NA') = COALESCE(UPPER(dbsref.salinvno),'NA'))) main) dbrcurr) a1dbrcur
      GROUP BY mon,
               yr,
               distcode,
               prdcode,
               nr,
               src,
               runmm,
               runyr,
               iscubeprocess,
               serno,
               dbrestore_nr_prev,
               dbrestore_nr_curr) ds
UNION ALL
SELECT srfinal.serno,
       srfinal.mon,
       srfinal.yr,
       srfinal.distcode,
       srfinal.prdcode,
       srfinal.nr,
       srfinal.prdqty,
       srfinal.prdgrossamt,
       srfinal.ptrvalue,
       srfinal.src,
       srfinal.prdnrvalue,
       srfinal.lpvalue,
       srfinal.trinqty,
       srfinal.troutqty,
       srfinal.trinval,
       srfinal.troutval,
       srfinal.nonwaveopenqty,
       srfinal.runmm,
       srfinal.runyr,
       srfinal.iscubeprocess,
       srfinal.pricelistlp,
       srfinal.pricelistptr,
       srfinal.prdqty_new,
       srfinal.prdnrvalue_new,
       srfinal.ptrvalue_new,
       srfinal.dbrestore_nr_prev,
       srfinal.dbrestore_prdqty_prev,
       srfinal.dbrestore_prdgrossamt_prev,
       srfinal.dbrestore_prdnrvalue_prev,
       srfinal.dbrestore_ptrvalue_prev,
       srfinal.dbrestore_nr_curr,
       srfinal.dbrestore_prdqty_curr,
       srfinal.dbrestore_prdgrossamt_curr,
       srfinal.dbrestore_prdnrvalue_curr,
       srfinal.dbrestore_ptrvalue_curr,
       convert_timezone('Asia/Kolkata',current_timestamp()) AS crt_dttm,
       convert_timezone('Asia/Kolkata',current_timestamp()) AS updt_dttm
FROM 
(
       SELECT sr.serno,
             sr.mon,
             sr.yr,
             sr.distcode,
             sr.prdcode,
             sr.nr,
             SUM(sr.prdqty) AS prdqty,
             SUM(sr.prdgrossamt) AS prdgrossamt,
             SUM(sr.ptrvalue) AS ptrvalue,
             sr.src,
             SUM(sr.prdnrvalue) AS prdnrvalue,
             SUM(sr.lpvalue) AS lpvalue,
             SUM(sr.trinqty) AS trinqty,
             SUM(sr.troutqty) AS troutqty,
             SUM(sr.trinval) AS trinval,
             SUM(sr.troutval) AS troutval,
             SUM(sr.nonwaveopenqty) AS nonwaveopenqty,
             sr.runmm,
             sr.runyr,
             sr.iscubeprocess,
             SUM(sr.pricelistlp) AS pricelistlp,
             SUM(sr.pricelistptr) AS pricelistptr,
             SUM(sr.prdqty_new) AS prdqty_new,
             SUM(sr.prdnrvalue_new) AS prdnrvalue_new,
             SUM(sr.ptrvalue_new) AS ptrvalue_new,
             sr.dbrestore_nr_prev,
             SUM(sr.dbrestore_prdqty_prev) AS dbrestore_prdqty_prev,
             SUM(sr.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_prev,
             SUM(sr.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_prev,
             SUM(sr.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_prev,
             sr.dbrestore_nr_curr,
             SUM(sr.dbrestore_prdqty_prev) AS dbrestore_prdqty_curr,
             SUM(sr.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_curr,
             SUM(sr.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_curr,
             SUM(sr.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_curr
      FROM (SELECT DISTINCT CAST(NULL AS BIGINT) AS serno,
                   cal_dim.mon,
                   cal_dim.yr,
                   srsales.distcode,
                   srsales.prdcode,
                   srsales.nr,
                   srsales.srndate,
                   srsales.mrp,
                   SUM(srsales.prdqty*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS prdqty,
                   SUM(srsales.prdgrossamt*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS prdgrossamt,
                   SUM(srsales.ptrvalue*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS ptrvalue,
                   srsales.src,
                   SUM(nr*srsales.prdqty*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS prdnrvalue,
                   0 AS lpvalue,
                   0 AS trinqty,
                   0 AS troutqty,
                   0 AS trinval,
                   0 AS troutval,
                   CAST(NULL AS INT) AS nonwaveopenqty,
                   cal_dim.mon AS runmm,
                   cal_dim.yr AS runyr,
                   CAST(NULL AS INT) AS iscubeprocess,
                   0 AS pricelistlp,
                   0 AS pricelistptr,
                   SUM(srsales.prdqty*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS prdqty_new,
                   SUM(nr*srsales.prdqty*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS prdnrvalue_new,
                   SUM(srsales.ptrvalue*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS ptrvalue_new,
                   0 AS dbrestore_nr_prev,
                   0 AS dbrestore_prdqty_prev,
                   0 AS dbrestore_prdgrossamt_prev,
                   0 AS dbrestore_prdnrvalue_prev,
                   0 AS dbrestore_ptrvalue_prev,
                   0 AS dbrestore_nr_curr,
                   0 AS dbrestore_prdqty_curr,
                   0 AS dbrestore_prdgrossamt_curr,
                   0 AS dbrestore_prdnrvalue_curr,
                   0 AS dbrestore_ptrvalue_curr
            FROM (SELECT distcode AS distcode,
                         prdcode,
                         (prdsalqty + prdunsalqty) AS prdqty,
                         ((prdsalqty + prdunsalqty)*(prdselrateaftertax)) AS ptrvalue,
                         prdgrossamt,
                         'SR' AS src,
                         nrvalue AS nr,
                         mrp,
                         srndate,
                         modifieddate
                  FROM itg_salesreturn) srsales
              INNER JOIN (SELECT DISTINCT customer_code
                          FROM edw_customer_dim
                          WHERE UPPER(psnonps) = 'Y'
                          AND   (UPPER(active_flag) = 'Y' or UPPER(active_flag) = 'N')
                          AND   customer_code <> '') cust ON LTRIM (srsales.distcode,'0') = cust.customer_code
              INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                 CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                 caldate
                          FROM edw_retailer_calendar_dim
                          WHERE mth_mm IN (SELECT (year||lpad (MONTH,2,0))
                                           FROM itg_mds_month_end_dates
                                           WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp())) <= to_date(pathfinder_month_end))) cal_dim ON to_date (srsales.srndate) = to_date (cal_dim.caldate)
              INNER JOIN (SELECT YEAR,
                                 MONTH,
                                 pathfinder_month_end
                          FROM itg_mds_month_end_dates) mon_end
                      ON mon_end.year = cal_dim.yr
                     AND mon_end.month = cal_dim.mon
            WHERE to_date(modifieddate) <= to_date(pathfinder_month_end)) sr
      GROUP BY mon,
               yr,
               distcode,
               prdcode,
               nr,
               src,
               runmm,
               runyr,
               iscubeprocess,
               serno,
               dbrestore_nr_prev,
               dbrestore_nr_curr
      UNION ALL
      SELECT srrunmm.serno,
             srrunmm.mon,
             srrunmm.yr,
             srrunmm.distcode,
             srrunmm.prdcode,
             srrunmm.nr,
             SUM(srrunmm.prdqty) AS prdqty,
             SUM(srrunmm.prdgrossamt) AS prdgrossamt,
             SUM(srrunmm.ptrvalue) AS ptrvalue,
             srrunmm.src,
             SUM(srrunmm.prdnrvalue) AS prdnrvalue,
             SUM(srrunmm.lpvalue) AS lpvalue,
             SUM(srrunmm.trinqty) AS trinqty,
             SUM(srrunmm.troutqty) AS troutqty,
             SUM(srrunmm.trinval) AS trinval,
             SUM(srrunmm.troutval) AS troutval,
             SUM(srrunmm.nonwaveopenqty) AS nonwaveopenqty,
             srrunmm.runmm,
             srrunmm.runyr,
             srrunmm.iscubeprocess,
             SUM(srrunmm.pricelistlp) AS pricelistlp,
             SUM(srrunmm.pricelistptr) AS pricelistptr,
             SUM(srrunmm.prdqty_new) AS prdqty_new,
             SUM(srrunmm.prdnrvalue_new) AS prdnrvalue_new,
             SUM(srrunmm.ptrvalue_new) AS ptrvalue_new,
             srrunmm.dbrestore_nr_prev,
             SUM(srrunmm.dbrestore_prdqty_prev) AS dbrestore_prdqty_prev,
             SUM(srrunmm.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_prev,
             SUM(srrunmm.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_prev,
             SUM(srrunmm.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_prev,
             srrunmm.dbrestore_nr_curr,
             SUM(srrunmm.dbrestore_prdqty_prev) AS dbrestore_prdqty_curr,
             SUM(srrunmm.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_curr,
             SUM(srrunmm.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_curr,
             SUM(srrunmm.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_curr
      FROM (SELECT DISTINCT CAST(NULL AS BIGINT) AS serno,
                   cal_dim.mon,
                   cal_dim.yr,
                   srsales.distcode,
                   srsales.prdcode,
                   srsales.nr,
                   SUM(srsales.prdqty*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS prdqty,
                   SUM(srsales.prdgrossamt*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS prdgrossamt,
                   SUM(srsales.ptrvalue*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS ptrvalue,
                   srsales.src,
                   SUM(nr*srsales.prdqty*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS prdnrvalue,
                   0 AS lpvalue,
                   0 AS trinqty,
                   0 AS troutqty,
                   0 AS trinval,
                   0 AS troutval,
                   CAST(NULL AS INT) AS nonwaveopenqty,
                   CAST(SUBSTRING(to_date(cal_dim.run_yearmonth),6,2) AS INT) AS runmm,
                   CAST(SUBSTRING(to_date(cal_dim.run_yearmonth),1,4) AS INT) AS runyr,
                   CAST(NULL AS INT) AS iscubeprocess,
                   0 AS pricelistlp,
                   0 AS pricelistptr,
                   SUM(srsales.prdqty*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS prdqty_new,
                   SUM(nr*srsales.prdqty*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS prdnrvalue_new,
                   SUM(srsales.ptrvalue*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS ptrvalue_new,
                   0 AS dbrestore_nr_prev,
                   0 AS dbrestore_prdqty_prev,
                   0 AS dbrestore_prdgrossamt_prev,
                   0 AS dbrestore_prdnrvalue_prev,
                   0 AS dbrestore_ptrvalue_prev,
                   0 AS dbrestore_nr_curr,
                   0 AS dbrestore_prdqty_curr,
                   0 AS dbrestore_prdgrossamt_curr,
                   0 AS dbrestore_prdnrvalue_curr,
                   0 AS dbrestore_ptrvalue_curr
            FROM (SELECT distcode AS distcode,
                         prdcode,
                         (prdsalqty + prdunsalqty) AS prdqty,
                         ((prdsalqty + prdunsalqty)*(prdselrateaftertax)) AS ptrvalue,
                         prdgrossamt,
                         'SR' AS src,
                         nrvalue AS nr,
                         mrp,
                         srndate,
                         modifieddate
                  FROM itg_salesreturn) srsales
              INNER JOIN (SELECT DISTINCT customer_code
                          FROM edw_customer_dim
                          WHERE psnonps = 'Y'
                          AND   (UPPER(active_flag) = 'Y' or UPPER(active_flag) = 'N')
                          AND   customer_code <> '') cust ON LTRIM (srsales.distcode,'0') = cust.customer_code
              INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                 CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                 add_months(TO_DATE(SUBSTRING(mth_mm,1,4) ||SUBSTRING (mth_mm,5,2),'YYYYMM'),1) AS run_yearmonth,
                                 caldate
                          FROM edw_retailer_calendar_dim
                          WHERE mth_mm IN (SELECT TO_CHAR(add_months (TO_DATE(concat (YEAR,LPAD(MONTH,2,0)),'YYYYMM'),-1),'YYYYMM')
                                           FROM itg_mds_month_end_dates
                                           WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp())) <= to_date(pathfinder_month_end))) cal_dim ON to_date (srsales.srndate) = to_date (cal_dim.caldate)
              INNER JOIN (SELECT YEAR,
                                 MONTH,
                                 pathfinder_month_end
                          FROM itg_mds_month_end_dates) mon_end
                      ON mon_end.year = cal_dim.yr
                     AND mon_end.month = cal_dim.mon
            WHERE to_date(modifieddate) > to_date(pathfinder_month_end)) srrunmm
      GROUP BY mon,
               yr,
               distcode,
               prdcode,
               nr,
               src,
               runmm,
               runyr,
               iscubeprocess,
               serno,
               dbrestore_nr_prev,
               dbrestore_nr_curr
      UNION ALL
      SELECT srdbres.serno,
             srdbres.mon,
             srdbres.yr,
             srdbres.distcode,
             srdbres.prdcode,
             srdbres.nr,
             SUM(srdbres.prdqty) AS prdqty,
             SUM(srdbres.prdgrossamt) AS prdgrossamt,
             SUM(srdbres.ptrvalue) AS ptrvalue,
             srdbres.src,
             SUM(srdbres.prdnrvalue) AS prdnrvalue,
             SUM(srdbres.lpvalue) AS lpvalue,
             SUM(srdbres.trinqty) AS trinqty,
             SUM(srdbres.troutqty) AS troutqty,
             SUM(srdbres.trinval) AS trinval,
             SUM(srdbres.troutval) AS troutval,
             SUM(srdbres.nonwaveopenqty) AS nonwaveopenqty,
             srdbres.runmm,
             srdbres.runyr,
             srdbres.iscubeprocess,
             SUM(srdbres.pricelistlp) AS pricelistlp,
             SUM(srdbres.pricelistptr) AS pricelistptr,
             SUM(srdbres.prdqty_new) AS prdqty_new,
             SUM(srdbres.prdnrvalue_new) AS prdnrvalue_new,
             SUM(srdbres.ptrvalue_new) AS ptrvalue_new,
             srdbres.dbrestore_nr_prev,
             SUM(srdbres.dbrestore_prdqty_prev) AS dbrestore_prdqty_prev,
             SUM(srdbres.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_prev,
             SUM(srdbres.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_prev,
             SUM(srdbres.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_prev,
             srdbres.dbrestore_nr_curr,
             SUM(srdbres.dbrestore_prdqty_prev) AS dbrestore_prdqty_curr,
             SUM(srdbres.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_curr,
             SUM(srdbres.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_curr,
             SUM(srdbres.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_curr
      FROM (SELECT DISTINCT CAST(NULL AS BIGINT) AS serno,
                   cal_dim.mon,
                   cal_dim.yr,
                   srsales.distcode,
                   srsales.prdcode,
                   0 AS nr,
                   0 AS prdqty,
                   0 AS prdgrossamt,
                   0 AS ptrvalue,
                   srsales.src,
                   0 AS prdnrvalue,
                   0 AS lpvalue,
                   0 AS trinqty,
                   0 AS troutqty,
                   0 AS trinval,
                   0 AS troutval,
                   CAST(NULL AS INT) AS nonwaveopenqty,
                   CAST(SUBSTRING(to_date(cal_dim.run_yearmonth),6,2) AS INT) AS runmm,
                   CAST(SUBSTRING(to_date(cal_dim.run_yearmonth),1,4) AS INT) AS runyr,
                   CAST(NULL AS INT) AS iscubeprocess,
                   0 AS pricelistlp,
                   0 AS pricelistptr,
                   0 AS prdqty_new,
                   0 AS prdnrvalue_new,
                   0 AS ptrvalue_new,
                   srsales.nr AS dbrestore_nr_prev,
                   SUM(srsales.prdqty*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS dbrestore_prdqty_prev,
                   SUM(srsales.prdgrossamt*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS dbrestore_prdgrossamt_prev,
                   SUM(srsales.nr*srsales.prdqty*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS dbrestore_prdnrvalue_prev,
                   SUM(srsales.ptrvalue*-1) OVER (PARTITION BY srsales.distcode,srsales.prdcode,srsales.srndate,srsales.mrp,srsales.nr) AS dbrestore_ptrvalue_prev,
                   0 AS dbrestore_nr_curr,
                   0 AS dbrestore_prdqty_curr,
                   0 AS dbrestore_prdgrossamt_curr,
                   0 AS dbrestore_prdnrvalue_curr,
                   0 AS dbrestore_ptrvalue_curr
            FROM (SELECT distcode AS distcode,
                         prdcode,
                         (prdsalqty + prdunsalqty) AS prdqty,
                         ((prdsalqty + prdunsalqty)*(prdselrateaftertax)) AS ptrvalue,
                         prdgrossamt,
                         'SR' AS src,
                         nrvalue AS nr,
                         mrp,
                         srndate,
                         createddate,
                         createddt
                  FROM itg_salesreturn_dbrestore) srsales
              INNER JOIN (SELECT DISTINCT customer_code
                          FROM edw_customer_dim
                          WHERE psnonps = 'Y'
                          AND   (UPPER(active_flag) = 'Y' or UPPER(active_flag) = 'N')
                          AND   customer_code <> '') cust ON LTRIM (srsales.distcode,'0') = cust.customer_code
              INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                 CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                 add_months(TO_DATE(SUBSTRING(mth_mm,1,4) ||SUBSTRING (mth_mm,5,2),'YYYYMM'),1) AS run_yearmonth,
                                 caldate
                          FROM edw_retailer_calendar_dim
                          WHERE mth_mm IN (SELECT TO_CHAR(add_months (TO_DATE(concat (YEAR,LPAD(MONTH,2,0)),'YYYYMM'),-1),'YYYYMM')
                                           FROM itg_mds_month_end_dates
                                           WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp())) <= to_date(pathfinder_month_end))) cal_dim ON to_date (srsales.srndate) = to_date (cal_dim.caldate)
              INNER JOIN (SELECT YEAR,
                                 MONTH,
                                 pathfinder_month_end
                          FROM itg_mds_month_end_dates) mon_end
                      ON mon_end.year = cal_dim.yr
                     AND mon_end.month = cal_dim.mon
            WHERE to_date(createddt) > to_date(pathfinder_month_end)) srdbres
      GROUP BY mon,
               yr,
               distcode,
               prdcode,
               nr,
               src,
               runmm,
               runyr,
               iscubeprocess,
               serno,
               dbrestore_nr_prev,
               dbrestore_nr_curr
      UNION ALL
      SELECT srrunmmdb.serno,
             srrunmmdb.mon,
             srrunmmdb.yr,
             srrunmmdb.distcode,
             srrunmmdb.prdcode,
             srrunmmdb.nr,
             SUM(srrunmmdb.prdqty) AS prdqty,
             SUM(srrunmmdb.prdgrossamt) AS prdgrossamt,
             SUM(srrunmmdb.ptrvalue) AS ptrvalue,
             srrunmmdb.src,
             SUM(srrunmmdb.prdnrvalue) AS prdnrvalue,
             SUM(srrunmmdb.lpvalue) AS lpvalue,
             SUM(srrunmmdb.trinqty) AS trinqty,
             SUM(srrunmmdb.troutqty) AS troutqty,
             SUM(srrunmmdb.trinval) AS trinval,
             SUM(srrunmmdb.troutval) AS troutval,
             SUM(srrunmmdb.nonwaveopenqty) AS nonwaveopenqty,
             srrunmmdb.runmm,
             srrunmmdb.runyr,
             srrunmmdb.iscubeprocess,
             SUM(srrunmmdb.pricelistlp) AS pricelistlp,
             SUM(srrunmmdb.pricelistptr) AS pricelistptr,
             SUM(srrunmmdb.prdqty_new) AS prdqty_new,
             SUM(srrunmmdb.prdnrvalue_new) AS prdnrvalue_new,
             SUM(srrunmmdb.ptrvalue_new) AS ptrvalue_new,
             srrunmmdb.dbrestore_nr_prev,
             SUM(srrunmmdb.dbrestore_prdqty_prev) AS dbrestore_prdqty_prev,
             SUM(srrunmmdb.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_prev,
             SUM(srrunmmdb.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_prev,
             SUM(srrunmmdb.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_prev,
             srrunmmdb.dbrestore_nr_curr,
             SUM(srrunmmdb.dbrestore_prdqty_prev) AS dbrestore_prdqty_curr,
             SUM(srrunmmdb.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_curr,
             SUM(srrunmmdb.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_curr,
             SUM(srrunmmdb.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_curr
      FROM (SELECT DISTINCT CAST(NULL AS BIGINT) AS serno,
                   srsalesdb.mon,
                   srsalesdb.yr,
                   srsalesdb.distcode,
                   srsalesdb.prdcode,
                   0 AS nr,
                   0 AS prdqty,
                   0 AS prdgrossamt,
                   0 AS ptrvalue,
                   srsalesdb.src,
                   0 AS prdnrvalue,
                   0 AS lpvalue,
                   0 AS trinqty,
                   0 AS troutqty,
                   0 AS trinval,
                   0 AS troutval,
                   CAST(NULL AS INT) AS nonwaveopenqty,
                   CAST(SUBSTRING(to_date(srsalesdb.run_yearmonth),6,2) AS INT) AS runmm,
                   CAST(SUBSTRING(to_date(srsalesdb.run_yearmonth),1,4) AS INT) AS runyr,
                   CAST(NULL AS INT) AS iscubeprocess,
                   0 AS pricelistlp,
                   0 AS pricelistptr,
                   0 AS prdqty_new,
                   0 AS prdnrvalue_new,
                   0 AS ptrvalue_new,
                   0 AS dbrestore_nr_prev,
                   0 AS dbrestore_prdqty_prev,
                   0 AS dbrestore_prdgrossamt_prev,
                   0 AS dbrestore_prdnrvalue_prev,
                   0 AS dbrestore_ptrvalue_prev,
                   srsalesdb.nr AS dbrestore_nr_curr,
                   SUM(srsalesdb.prdqty*-1) OVER (PARTITION BY srsalesdb.distcode,srsalesdb.prdcode,srsalesdb.srndate,srsalesdb.mrp,srsalesdb.nr) AS dbrestore_prdqty_curr,
                   SUM(srsalesdb.prdgrossamt*-1) OVER (PARTITION BY srsalesdb.distcode,srsalesdb.prdcode,srsalesdb.srndate,srsalesdb.mrp,srsalesdb.nr) AS dbrestore_prdgrossamt_curr,
                   SUM(srsalesdb.nr*srsalesdb.prdqty*-1) OVER (PARTITION BY srsalesdb.distcode,srsalesdb.prdcode,srsalesdb.srndate,srsalesdb.mrp,srsalesdb.nr) AS dbrestore_prdnrvalue_curr,
                   SUM(srsalesdb.ptrvalue*-1) OVER (PARTITION BY srsalesdb.distcode,srsalesdb.prdcode,srsalesdb.srndate,srsalesdb.mrp,srsalesdb.nr) AS dbrestore_ptrvalue_curr
            FROM (SELECT sr.distcode,
                         sr.prdcode,
                         (sr.prdsalqty + sr.prdunsalqty) AS prdqty,
                         ((sr.prdsalqty + sr.prdunsalqty)*(sr.prdselrateaftertax)) AS ptrvalue,
                         sr.prdgrossamt,
                         'SR' AS src,
                         sr.nrvalue AS nr,
                         sr.mrp,
                         sr.srndate,
                         sr.modifieddate,
                         sr.prdsalinvno,
                         cal_dim.mon,
                         cal_dim.yr,
                         cal_dim.run_yearmonth
                  FROM itg_salesreturn sr
                    INNER JOIN (SELECT DISTINCT customer_code
                                FROM edw_customer_dim
                                WHERE psnonps = 'Y'
                                AND   (UPPER(active_flag) = 'Y' or UPPER(active_flag) = 'N')
                                AND   customer_code <> '') cust ON LTRIM (sr.distcode,'0') = cust.customer_code
                    INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                       CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                       add_months(TO_DATE(SUBSTRING(mth_mm,1,4) ||SUBSTRING (mth_mm,5,2),'YYYYMM'),1) AS run_yearmonth,
                                       caldate
                                FROM edw_retailer_calendar_dim
                                WHERE mth_mm IN (SELECT TO_CHAR(add_months (TO_DATE(concat (YEAR,LPAD(MONTH,2,0)),'YYYYMM'),-1),'YYYYMM')
                                                 FROM itg_mds_month_end_dates
                                                 WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp())) <= to_date(pathfinder_month_end))) cal_dim ON to_date (sr.srndate) = to_date (cal_dim.caldate)
                    INNER JOIN (SELECT YEAR,
                                       MONTH,
                                       pathfinder_month_end
                                FROM itg_mds_month_end_dates) mon_end
                            ON mon_end.year = cal_dim.yr
                           AND mon_end.month = cal_dim.mon
                  WHERE to_date(modifieddate) > to_date(pathfinder_month_end)
                  AND   EXISTS (SELECT 1
                                FROM (SELECT DISTINCT distcode,
                                             prdsalinvno
                                      FROM itg_salesreturn_dbrestore srsalesdb
                                        INNER JOIN (SELECT DISTINCT customer_code
                                                    FROM edw_customer_dim
                                                    WHERE psnonps = 'Y'
                                                    AND   (UPPER(active_flag) = 'Y' or UPPER(active_flag) = 'N')
                                                    AND   customer_code <> '') cust ON LTRIM (srsalesdb.distcode,'0') = cust.customer_code
                                        INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                                           CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                                           add_months(TO_DATE(SUBSTRING(mth_mm,1,4) ||SUBSTRING (mth_mm,5,2),'YYYYMM'),1) AS run_yearmonth,
                                                           caldate
                                                    FROM edw_retailer_calendar_dim
                                                    WHERE mth_mm IN (SELECT TO_CHAR(add_months (TO_DATE(concat (YEAR,LPAD(MONTH,2,0)),'YYYYMM'),-1),'YYYYMM')
                                                                     FROM itg_mds_month_end_dates
                                                                     WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp())) <= to_date(pathfinder_month_end))) cal_dim ON to_date (srsalesdb.srndate) = to_date (cal_dim.caldate)
                                        INNER JOIN (SELECT YEAR,
                                                           MONTH,
                                                           pathfinder_month_end
                                                    FROM itg_mds_month_end_dates) mon_end
                                                ON mon_end.year = cal_dim.yr
                                               AND mon_end.month = cal_dim.mon
                                      WHERE to_date(createddt) > to_date(pathfinder_month_end)) srdb
                                WHERE COALESCE(sr.distcode,'NA') = COALESCE(srdb.distcode,'NA')
                                AND   COALESCE(UPPER(sr.prdsalinvno),'NA') = COALESCE(UPPER(srdb.prdsalinvno),'NA'))) srsalesdb) srrunmmdb
      GROUP BY mon,
               yr,
               distcode,
               prdcode,
               nr,
               src,
               runmm,
               runyr,
               iscubeprocess,
               serno,
               dbrestore_nr_prev,
               dbrestore_nr_curr) srfinal
UNION ALL
SELECT DISTINCT bill_fact.serno,
       bill_fact.mon,
       bill_fact.yr,
       bill_fact.distcode,
       bill_fact.prdcode,
       bill_fact.nr,
       bill_fact.prdqty,
       bill_fact.prdgrossamt,
       bill_fact.ptrvalue,
       bill_fact.src,
       bill_fact.prdnrvalue,
       bill_fact.lpvalue,
       bill_fact.trinqty,
       bill_fact.troutqty,
       bill_fact.trinval,
       bill_fact.troutval,
       bill_fact.nonwaveopenqty,
       bill_fact.runmm,
       bill_fact.runyr,
       bill_fact.iscubeprocess,
       bill_fact.pricelistlp,
       bill_fact.pricelistptr,
       bill_fact.prdqty_new,
       bill_fact.prdnrvalue_new,
       bill_fact.ptrvalue_new,
       bill_fact.dbrestore_nr_prev,
       bill_fact.dbrestore_prdqty_prev,
       bill_fact.dbrestore_prdgrossamt_prev,
       bill_fact.dbrestore_prdnrvalue_prev,
       bill_fact.dbrestore_ptrvalue_prev,
       bill_fact.dbrestore_nr_curr,
       bill_fact.dbrestore_prdqty_curr,
       bill_fact.dbrestore_prdgrossamt_curr,
       bill_fact.dbrestore_prdnrvalue_curr,
       bill_fact.dbrestore_ptrvalue_curr,
       convert_timezone('Asia/Kolkata',current_timestamp()) AS crt_dttm,
       convert_timezone('Asia/Kolkata',current_timestamp()) AS updt_dttm
FROM (SELECT a1.serno,
             a1.mon,
             a1.yr,
             a1.distcode,
             a1.prdcode,
             a1.nr,
             SUM(a1.prdqty) AS prdqty,
             SUM(a1.prdgrossamt) AS prdgrossamt,
             SUM(a1.ptrv) AS ptrvalue,
             a1.src,
             SUM(a1.prdgrossamt) AS prdnrvalue,
             SUM(a1.lpvalue) AS lpvalue,
             SUM(a1.trinqty) AS trinqty,
             SUM(a1.troutqty) AS troutqty,
             SUM(a1.trinval) AS trinval,
             SUM(a1.troutval) AS troutval,
             SUM(a1.nonwaveopenqty) AS nonwaveopenqty,
             a1.runmm,
             a1.runyr,
             a1.iscubeprocess,
             SUM(a1.pricelistlp) AS pricelistlp,
             SUM(a1.pricelistptr) AS pricelistptr,
             SUM(a1.prdqty) AS prdqty_new,
             SUM(a1.prdgrossamt) AS prdnrvalue_new,
             SUM(a1.ptrv) AS ptrvalue_new,
             a1.dbrestore_nr_prev,
             SUM(a1.dbrestore_prdqty_prev) AS dbrestore_prdqty_prev,
             SUM(a1.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_prev,
             SUM(a1.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_prev,
             SUM(a1.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_prev,
             a1.dbrestore_nr_curr,
             SUM(a1.dbrestore_prdqty_prev) AS dbrestore_prdqty_curr,
             SUM(a1.dbrestore_prdgrossamt_prev) AS dbrestore_prdgrossamt_curr,
             SUM(a1.dbrestore_prdnrvalue_prev) AS dbrestore_prdnrvalue_curr,
             SUM(a1.dbrestore_ptrvalue_prev) AS dbrestore_ptrvalue_curr
      FROM (SELECT CAST(NULL AS BIGINT) AS serno,
                   billing.mon,
                   billing.yr,
                   billing.distcode,
                   billing.prdcode,
                   CAST(billing.subtotal_4 / NULLIF(billing.bill_qty,0) AS NUMERIC(12,3)) AS nr,
                   billing.bill_qty AS prdqty,
                   (billing.subtotal_4 / NULLIF(billing.bill_qty,0))*billing.bill_qty AS prdgrossamt,
                   bill_qty AS ptrvalue,
                   billing.src,
                   0 AS lpvalue,
                   0 AS trinqty,
                   0 AS troutqty,
                   0 AS trinval,
                   0 AS troutval,
                   created_on,
                   CAST(NULL AS INT) AS nonwaveopenqty,
                   billing.mon AS runmm,
                   billing.yr AS runyr,
                   CAST(NULL AS INT) AS iscubeprocess,
                   0 AS pricelistlp,
                   0 AS pricelistptr,
                   billing.bill_qty AS prdqty_new,
                   bill_qty*ptr AS ptrv,
                   0 AS dbrestore_nr_prev,
                   0 AS dbrestore_prdqty_prev,
                   0 AS dbrestore_prdgrossamt_prev,
                   0 AS dbrestore_prdnrvalue_prev,
                   0 AS dbrestore_ptrvalue_prev,
                   0 AS dbrestore_nr_curr,
                   0 AS dbrestore_prdqty_curr,
                   0 AS dbrestore_prdgrossamt_curr,
                   0 AS dbrestore_prdnrvalue_curr,
                   0 AS dbrestore_ptrvalue_curr
            FROM ((SELECT inv.distcode,
                          inv.prdcode,
                          inv.bill_qty,
                          inv.src,
                          inv.created_on,
                          inv.subtotal_4,
                          cust.state_code,
                          cal_dim.mon AS runmm,
                          cal_dim.yr AS runyr,
                          cal_dim.mon,
                          cal_dim.yr
                   FROM (SELECT LTRIM(sold_to,0) AS distcode,
                                LTRIM(material,0) AS prdcode,
                                bill_qty,
                                'SNS' AS src,
                                created_on,
                                subtotal_4
                         FROM edw_billing_fact
                         WHERE UPPER(bill_type) IN ('S1','S2','ZC22','ZC2D','ZF2D','ZG22','ZG2D','ZL2D','ZL22','ZC3D','ZF2E','ZL3D','ZG3D','ZRSM','ZSMD')
                         AND   sls_org = '5100') inv
                     INNER JOIN (SELECT DISTINCT customer_code,
                                        state_code
                                 FROM edw_customer_dim
                                 WHERE UPPER(direct_account_flag) = 'Y'
                                 AND   (UPPER(active_flag) = 'Y' or UPPER(active_flag) = 'N')
                                 AND   UPPER(psnonps) = 'N'
                                 AND   customer_code <> '') cust ON LTRIM (inv.distcode,'0') = cust.customer_code
                     INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                                        CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                                        caldate
                                 FROM edw_retailer_calendar_dim
                                 WHERE mth_mm IN (SELECT (year||lpad (MONTH,2,0))
                                                  FROM itg_mds_month_end_dates
                                                  WHERE to_date(convert_timezone('Asia/Kolkata',current_timestamp())) <= to_date(pathfinder_month_end))) cal_dim ON to_date (inv.created_on) = to_date (cal_dim.caldate)) billing
                     LEFT JOIN (SELECT DISTINCT productcode,
                                       statecode,
                                       CASE
                                         WHEN billingunit = 0 THEN 0
                                         ELSE CAST(retailerprice / billingunit AS NUMERIC(16,3))
                                       END AS ptr,
                                       startDate,
                                       insertedOn,
                                       MrpPerPack,
                                       ROW_NUMBER() OVER (PARTITION BY productcode,statecode ORDER BY startDate DESC,insertedOn DESC,MrpPerPack) AS rk
                                FROM itg_pricelist) pl
                            ON LTRIM (pl.productcode,0) = LTRIM (billing.prdcode,0)
                           AND rk = 1
                           AND pl.statecode = billing.state_code)) a1
      GROUP BY mon,
               yr,
               distcode,
               prdcode,
               nr,
               src,
               runmm,
               runyr,
               iscubeprocess,
               serno,
               dbrestore_nr_prev,
               dbrestore_nr_curr) bill_fact
)
,final as 
(
    select 
	serno::number(38,0) as serno,
	mon::number(18,0) as mon,
	yr::number(18,0) as yr,
	distcode::varchar(50) as distcode,
	prdcode::varchar(50) as prdcode,
	nr::number(12,4) as nr,
	prdqty::number(16,4) as prdqty,
	prdgrossamt::number(16,4) as prdgrossamt,
	ptrvalue::number(16,4) as ptrvalue,
	src::varchar(5) as src,
	prdnrvalue::number(18,3) as prdnrvalue,
	lpvalue::number(18,3) as lpvalue,
	trinqty::number(16,3) as trinqty,
	troutqty::number(16,3) as troutqty,
	trinval::number(16,3) as trinval,
	troutval::number(16,3) as troutval,
	nonwaveopenqty::number(16,3) as nonwaveopenqty,
	runmm::number(18,0) as runmm,
	runyr::number(18,0) as runyr,
	iscubeprocess::varchar(1) as iscubeprocess,
	pricelistlp::number(16,3) as pricelistlp,
	pricelistptr::number(16,3) as pricelistptr,
	prdqty_new::number(16,4) as prdqty_new,
	prdnrvalue_new::number(16,4) as prdnrvalue_new,
	ptrvalue_new::number(16,4) as ptrvalue_new,
	dbrestore_nr_prev::number(12,4) as dbrestore_nr_prev,
	dbrestore_prdqty_prev::number(16,4) as dbrestore_prdqty_prev,
	dbrestore_prdgrossamt_prev::number(16,4) as dbrestore_prdgrossamt_prev,
	dbrestore_prdnrvalue_prev::number(16,4) as dbrestore_prdnrvalue_prev,
	dbrestore_ptrvalue_prev::number(16,4) as dbrestore_ptrvalue_prev,
	dbrestore_nr_curr::number(12,4) as dbrestore_nr_current,
	dbrestore_prdqty_curr::number(16,4) as dbrestore_prdqty_current,
	dbrestore_prdgrossamt_curr::number(16,4) as dbrestore_prdgrossamt_current,
	dbrestore_prdnrvalue_curr::number(16,4) as dbrestore_prdnrvalue_current,
	dbrestore_ptrvalue_curr::number(16,4) as dbrestore_ptrvalue_current,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final