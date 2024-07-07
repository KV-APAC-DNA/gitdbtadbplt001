with 
itg_dailysales as 
(
    select * from {{ ref('inditg_integration__itg_dailysales') }}
),
itg_salesreturn as 
(
    select * from {{ ref('inditg_integration__itg_salesreturn') }}
),
itg_customer_retailer as 
(
    select * from {{ ref('inditg_integration__itg_customer_retailer') }}
),
itg_mds_in_sv_winculum_master as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_sv_winculum_master') }}
),
itg_winculum_dailysales as 
(
    select * from {{ ref('inditg_integration__itg_winculum_dailysales') }}
),
itg_mds_month_end_dates as 
(
    select * from {{ ref('inditg_integration__itg_mds_month_end_dates') }}
),
itg_winculum_salesreturn as 
(
    select * from {{ ref('inditg_integration__itg_winculum_salesreturn') }}
),
edw_retailer_calendar_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
edw_billing_fact as 
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
trans as 
(
    SELECT ins.keyaccountid,
       ins.distributorcode,
       ins.salinvno,
       ins.salinvdate,
       ins.dlvsts,
       ins.rtrcode,
       ins.rtrnm,
       ins.productid,
       ins.prdccode,
       ins.productname,
       ins.prdqty,
       ins.prdsalerate,
       ins.motherskuid,
       ins.motherskuname,
       ins.ctgtypid,
       ins.ctgtypdsc,
       ins.dlvsts2,
       ins.prdtaxamt,
       ins.mnfid,
       ins.prdschdiscamt,
       ins.prddbdiscamt,
       ins.salwdsamt,
       ins.discount,
       ins.schid,
       ins.createddate,
       cal_dim.fromdate,
       cal_dim.todate,
       ins.netrate AS netrate,
       ins.saleflag,
	   cal_dim.weekno,
       cust_ret.confirm_flag AS confirmsales,
       ins.subtotal4,
       current_timestamp() as crt_dttm
FROM (SELECT CAST(NULL AS INTEGER) AS keyaccountid,
             distcode AS distributorcode,
             salinvno,
             salinvdate,
             1 AS dlvsts,
             rtrcode,
             rtrname AS rtrnm,
             NULL AS productid,
             prdcode AS prdccode,
             NULL AS productname,
             prdqty,
             prdselratebeforetax AS prdsalerate,
             NULL AS motherskuid,
             NULL AS motherskuname,
             NULL AS ctgtypid,
             NULL AS ctgtypdsc,
             1 AS dlvsts2,
             prdtaxamt,
             1 AS mnfid,
             prdschdiscamt,
             prddbdiscamt,
             CAST(salwdsamt AS NUMERIC) AS salwdsamt,
             0 AS discount,
             CAST(NULL AS INTEGER) AS schid,
             createddate,
             nrvalue AS netrate,
             'IS' AS saleflag,
             CAST(NULL AS NUMERIC) AS subtotal4
             FROM itg_dailysales) ins
  INNER JOIN (SELECT DISTINCT wholesalercode,
                     distributorsapid,
                     CASE
                       WHEN isconfirm = 'C' THEN 'Y'
                       WHEN isconfirm = 'U' THEN 'N'
                       ELSE isconfirm
                     END AS confirm_flag
              FROM itg_customer_retailer
              WHERE isdirectacct = 'N'
              AND   wholesalercode IS NOT NULL
              AND   wholesalercode <> ''
              AND   distributorsapid IS NOT NULL
              AND   distributorsapid <> ''
              AND   nkacstores = 'Y') cust_ret
          ON ins.distributorcode = cust_ret.distributorsapid
         AND ins.rtrcode = cust_ret.wholesalercode
  INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                     CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                     MIN(caldate) OVER (PARTITION BY mth_mm) AS fromdate,
                     MAX(caldate) OVER (PARTITION BY mth_mm) AS todate,
                     caldate,
					 week as weekno
              FROM edw_retailer_calendar_dim where mth_mm in 
              (select concat( year, (CASE WHEN len(month) = 1 THEN concat(0,month) WHEN len(month) = 2 THEN CAST(month AS TEXT) END) ) from itg_mds_month_end_dates 
              where to_date(current_timestamp()) <= to_date(key_account_month_end))) cal_dim 
              ON to_date(ins.salinvdate) = to_date(cal_dim.caldate)
  INNER JOIN (SELECT YEAR,
                     MONTH,
                     key_account_month_end
              FROM itg_mds_month_end_dates) mon_end
          ON mon_end.year = cal_dim.yr
         AND mon_end.month = cal_dim.mon
WHERE to_date(createddate) <= to_date(key_account_month_end)
UNION ALL
SELECT inr.keyaccountid,
       inr.distributorcode,
       inr.salinvno,
       inr.salinvdate,
       inr.dlvsts,
       inr.rtrcode,
       inr.rtrnm,
       inr.productid,
       inr.prdccode,
       inr.productname,
       inr.prdqty,
       inr.prdsalerate,
       inr.motherskuid,
       inr.motherskuname,
       inr.ctgtypid,
       inr.ctgtypdsc,
       inr.dlvsts2,
       inr.prdtaxamt,
       inr.mnfid,
       inr.prdschdiscamt,
       inr.prddbdiscamt,
       inr.salwdsamt,
       inr.discount,
       inr.schid,
       inr.createddate,
       cal_dim.fromdate,
       cal_dim.todate,
       inr.netrate AS netrate,
       inr.saleflag,
	   cal_dim.weekno,
       cust_ret.confirm_flag AS confirmsales,
       --inr.confirmsales,
       inr.subtotal4,
       current_timestamp() as crt_dttm
FROM (SELECT CAST(NULL AS INTEGER) AS keyaccountid,
             distcode AS distributorcode,
             srnrefno AS salinvno,
             srndate AS salinvdate,
             1 AS dlvsts,
             rtrcode,
             rtrname AS rtrnm,
             NULL AS productid,
             prdcode AS prdccode,
             NULL AS productname,
             CAST((prdsalqty + prdunsalqty)*-1 AS INTEGER) AS prdqty,
             prdselrate AS prdsalerate,
             NULL AS motherskuid,
             NULL AS motherskuname,
             NULL AS ctgtypid,
             NULL AS ctgtypdsc,
             1 AS dlvsts2,
             (prdtaxamt*-1) AS prdtaxamt,
             1 AS mnfid,
             prdschdiscamt,
             prddbdiscamt,
             CAST(0 AS NUMERIC) AS salwdsamt,
             0 AS discount,
             CAST(NULL AS INTEGER) AS schid,
             createddate,
             nrvalue AS netrate,
             'IR' AS saleflag,
             CAST(0 AS NUMERIC) AS subtotal4
      FROM itg_salesreturn) inr
  INNER JOIN (SELECT DISTINCT distributorsapid,
                     wholesalercode,
                     CASE
                       WHEN isconfirm = 'C' THEN 'Y'
                       WHEN isconfirm = 'U' THEN 'N'
                       ELSE isconfirm
                     END AS confirm_flag
              FROM itg_customer_retailer
              WHERE isdirectacct = 'N'
              AND   wholesalercode IS NOT NULL
              AND   wholesalercode <> ''
              AND   distributorsapid IS NOT NULL
              AND   distributorsapid <> ''
              AND   nkacstores = 'Y') cust_ret
          ON inr.distributorcode = cust_ret.distributorsapid
         AND inr.rtrcode = cust_ret.wholesalercode
  INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                     CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                     MIN(caldate) OVER (PARTITION BY mth_mm) AS fromdate,
                     MAX(caldate) OVER (PARTITION BY mth_mm) AS todate,
                     caldate,
					 week as weekno
              FROM edw_retailer_calendar_dim where mth_mm in 
              (select concat( year, (CASE WHEN len(month) = 1 THEN concat(0,month) WHEN len(month) = 2 THEN CAST(month AS TEXT) END) ) from itg_mds_month_end_dates 
              where to_date(current_timestamp()) <= to_date(key_account_month_end))) cal_dim 
              ON to_date(inr.salinvdate) = to_date(cal_dim.caldate)
  INNER JOIN (SELECT YEAR,
                     MONTH,
                     key_account_month_end
              FROM itg_mds_month_end_dates) mon_end
          ON mon_end.year = cal_dim.yr
         AND mon_end.month = cal_dim.mon
WHERE to_date(createddate) <= to_date(key_account_month_end)
UNION ALL
SELECT bill.keyaccountid,
       bill.distributorcode,
       bill.salinvno,
       bill.salinvdate,
       bill.dlvsts,
       bill.rtrcode,
       bill.rtrnm,
       bill.productid,
       bill.prdccode,
       bill.productname,
       bill.prdqty,
       bill.prdsalerate,
       bill.motherskuid,
       bill.motherskuname,
       bill.ctgtypid,
       bill.ctgtypdsc,
       bill.dlvsts2,
       bill.prdtaxamt,
       bill.mnfid,
       bill.prdschdiscamt,
       bill.prddbdiscamt,
       bill.salwdsamt,
       bill.discount,
       bill.schid,
       bill.createddate,
       cal_dim.fromdate,
       cal_dim.todate,
       bill.netrate AS netrate,
       bill.saleflag,
	   cal_dim.weekno,
       cust_ret.confirm_flag AS confirmsales,
       bill.subtotal4,
       current_timestamp() as crt_dttm
FROM (SELECT CAST(NULL AS INTEGER) AS keyaccountid,
             LTRIM(sold_to,'0') AS distributorcode,
             bill_num AS salinvno,
             created_on AS salinvdate,
             CAST(NULL AS INTEGER) AS dlvsts,
             NULL AS rtrcode,
             NULL AS rtrnm,
             NULL AS productid,
             material AS prdccode,
             NULL AS productname,
             CAST(bill_qty AS INTEGER) AS prdqty,
             0 AS prdsalerate,
             NULL AS motherskuid,
             NULL AS motherskuname,
             NULL AS ctgtypid,
             NULL AS ctgtypdsc,
             CAST(NULL AS INTEGER) AS dlvsts2,
             tax_amt AS prdtaxamt,
             CAST(NULL AS INTEGER) AS mnfid,
             0 AS prdschdiscamt,
             0 AS prddbdiscamt,
             CAST(0 AS NUMERIC) AS salwdsamt,
             0 AS discount,
             CAST(NULL AS INTEGER) AS schid,
             CAST(NULL AS TIMESTAMP) AS createddate,
             subtotal_4 / bill_qty AS netrate,
             'DS' AS saleflag,
             NULLIF(subtotal_4,0) AS subtotal4
      FROM edw_billing_fact
      WHERE bill_type IN ('S1','S2','ZC22','ZC2D','ZF2D','ZG22','ZG2D','ZL2D','ZL22','ZC3D','ZF2E','ZL3D','ZG3D')
      AND   sls_org = '5100'
      AND   record_mode = 'N') bill
  INNER JOIN (SELECT DISTINCT sapid,
                     CASE
                       WHEN isconfirm = 'C' THEN 'Y'
                       WHEN isconfirm = 'U' THEN 'N'
                       ELSE isconfirm
                     END AS confirm_flag
              FROM
              itg_customer_retailer
              WHERE isdirectacct = 'Y') cust_ret ON LTRIM (bill.distributorcode,'0') = cust_ret.sapid
  INNER JOIN (SELECT MIN(caldate) OVER (PARTITION BY mth_mm) AS fromdate,
                     MAX(caldate) OVER (PARTITION BY mth_mm) AS todate,
                     caldate,
					 week as weekno
              FROM edw_retailer_calendar_dim where mth_mm in 
              (select concat( year, (CASE WHEN len(month) = 1 THEN concat(0,month) WHEN len(month) = 2 THEN CAST(month AS TEXT) END) ) from itg_mds_month_end_dates 
              where to_date(current_timestamp()) <= to_date(key_account_month_end))) cal_dim 
              ON to_date(bill.salinvdate) = to_date(cal_dim.caldate)
UNION ALL
SELECT Winds.keyaccountid,
       Winds.distributorcode,
       Winds.salinvno,
       Winds.salinvdate,
       Winds.dlvsts,
       Winds.rtrcode,
       cust_ret.customername as rtrnm,
       Winds.productid,
       Winds.prdccode,
       Winds.productname,
       Winds.prdqty,
       Winds.prdsalerate,
       Winds.motherskuid,
       Winds.motherskuname,
       Winds.ctgtypid,
       Winds.ctgtypdsc,
       Winds.dlvsts2,
       Winds.prdtaxamt,
       Winds.mnfid,
       Winds.prdschdiscamt,
       Winds.prddbdiscamt,
       Winds.salwdsamt,
       Winds.discount,
       Winds.schid,
       Winds.createddate,
       cal_dim.fromdate,
       cal_dim.todate,
       Winds.netrate AS netrate,
       Winds.saleflag,
	   cal_dim.weekno,
       'Y' AS confirmsales,
       Winds.subtotal4,
       current_timestamp() as crt_dttm
FROM (SELECT  CAST(NULL AS INTEGER) AS keyaccountid,
             distcode AS distributorcode,
             salinvno,
             salinvdate,
             1 AS dlvsts,
             rtrcode,
             null as rtrnm,
             -- not there
             NULL AS productid,
             productcode AS prdccode,
             NULL AS productname,
             prdqty,
             0 AS prdsalerate,
             NULL AS motherskuid,
             NULL AS motherskuname,
             NULL AS ctgtypid,
             NULL AS ctgtypdsc,
             1 AS dlvsts2,
             tax AS prdtaxamt,-- having tax
             0 AS mnfid,
             0 AS prdschdiscamt,
             0 AS prddbdiscamt,
             CAST(0 AS NUMERIC) AS salwdsamt,
             0 AS discount,
             CAST(NULL AS INTEGER) AS schid,
             cast(Null as timestamp) as createddate,-- not there,null 
             nr AS netrate,
             'WIS' AS saleflag,
             CAST(NULL AS NUMERIC) AS subtotal4
      FROM itg_winculum_dailysales
	   where distcode not in (select distinct distributorsapid from itg_mds_in_sv_winculum_master where active= 'Y')) Winds--to stop the winclum flow
  INNER JOIN (SELECT DISTINCT wholesalercode,
                     distributorsapid,customername        
              FROM itg_customer_retailer
              WHERE isdirectacct = 'W'
              AND   wholesalercode IS NOT NULL
              AND   wholesalercode <> ''
              --AND   isactive = 'Y'
              --AND   nkacstores = 'Y'
			  AND   distributorsapid IS NOT NULL
              AND   distributorsapid <> ''
              ) cust_ret
          ON Winds.distributorcode = cust_ret.distributorsapid
         AND Winds.rtrcode = cust_ret.wholesalercode
  INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                     CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                     MIN(caldate) OVER (PARTITION BY mth_mm) AS fromdate,
                     MAX(caldate) OVER (PARTITION BY mth_mm) AS todate,
                     caldate,
					 week as weekno
              FROM edw_retailer_calendar_dim where mth_mm in 
              (select concat( year, (CASE WHEN len(month) = 1 THEN concat(0,month) WHEN len(month) = 2 THEN CAST(month AS TEXT) END) ) from itg_mds_month_end_dates 
              where to_date(current_timestamp()) <= to_date(key_account_month_end))
            
 ) cal_dim 
              ON to_date(Winds.salinvdate) = to_date(cal_dim.caldate)
UNION ALL
SELECT winsr.keyaccountid,
       winsr.distributorcode,
       winsr.salinvno,
       winsr.salinvdate,
       winsr.dlvsts,
       winsr.rtrcode,
       cust_ret.customername as rtrnm,
       winsr.productid,
       winsr.prdccode,
       winsr.productname,
       winsr.prdqty,
       winsr.prdsalerate,
       winsr.motherskuid,
       winsr.motherskuname,
       winsr.ctgtypid,
       winsr.ctgtypdsc,
       winsr.dlvsts2,
       winsr.prdtaxamt,
       winsr.mnfid,
       winsr.prdschdiscamt,
       winsr.prddbdiscamt,
       winsr.salwdsamt,
       winsr.discount,
       winsr.schid,
       winsr.createddate,
       cal_dim.fromdate,
       cal_dim.todate,
       winsr.netrate AS netrate,
       winsr.saleflag,
	   cal_dim.weekno,
       'Y' AS confirmsales,
       --winsr.confirmsales,
       winsr.subtotal4,
       current_timestamp() as crt_dttm
FROM (SELECT CAST(NULL AS INTEGER) AS keyaccountid,
             distcode AS distributorcode,
             srnrefno AS salinvno,
             srndate AS salinvdate,
             1 AS dlvsts,
             rtrcode,
             null as rtrnm,
             -- not there
             NULL AS productid,
             productcode AS prdccode,
             NULL AS productname,
             (prdqty*-1) AS prdqty,--cast((prdsalqty + prdunsalqty)*-1 as integer) as prdqty,
             0 AS prdsalerate,
             NULL AS motherskuid,
             NULL AS motherskuname,
             NULL AS ctgtypid,
             NULL AS ctgtypdsc,
             1 AS dlvsts2,
             tax AS prdtaxamt,-- having tax
             0 AS mnfid,
             0 AS prdschdiscamt,
             0 AS prddbdiscamt,
             CAST(0 AS NUMERIC) AS salwdsamt,
             0 AS discount,
             CAST(NULL AS INTEGER) AS schid,
             cast(Null as timestamp) as createddate,
             nr AS netrate,
             'WIR' AS saleflag,
             CAST(0 AS NUMERIC) AS subtotal4
      FROM itg_winculum_salesreturn) winsr
  INNER JOIN (SELECT DISTINCT distributorsapid,
                     wholesalercode,customername
              FROM itg_customer_retailer
              WHERE isdirectacct = 'W'
              AND   wholesalercode IS NOT NULL
              AND   wholesalercode <> ''
              AND   distributorsapid IS NOT NULL
              AND   distributorsapid <> ''
              ) cust_ret
          ON winsr.distributorcode = cust_ret.distributorsapid
         AND winsr.rtrcode = cust_ret.wholesalercode
  INNER JOIN (SELECT CAST(SUBSTRING(mth_mm,5,2) AS INT) AS mon,
                     CAST(SUBSTRING(mth_mm,1,4) AS INT) AS yr,
                     MIN(caldate) OVER (PARTITION BY mth_mm) AS fromdate,
                     MAX(caldate) OVER (PARTITION BY mth_mm) AS todate,
                     caldate,
					 week as weekno
              FROM edw_retailer_calendar_dim where mth_mm in 
              (select concat( year, (CASE WHEN len(month) = 1 THEN concat(0,month) WHEN len(month) = 2 THEN CAST(month AS TEXT) END) ) from itg_mds_month_end_dates 
              where to_date(current_timestamp()) <= to_date(key_account_month_end))
              ) cal_dim 
              ON to_date(winsr.salinvdate) = to_date(cal_dim.caldate)
)
,final as 
(
    select 
    keyaccountid::number(18,0) as keyaccountid,
	distributorcode::varchar(10) as distributorcode,
	salinvno::varchar(100) as salinvno,
	salinvdate::timestamp_ntz(9) as salinvdate,
	dlvsts::number(18,0) as dlvsts,
	rtrcode::varchar(50) as rtrcode,
	rtrnm::varchar(200) as rtrnm,
	productid::varchar(15) as productid,
	prdccode::varchar(50) as prdccode,
	productname::varchar(200) as productname,
	prdqty::number(18,0) as prdqty,
	prdsalerate::number(38,6) as prdsalerate,
	motherskuid::varchar(15) as motherskuid,
	motherskuname::varchar(50) as motherskuname,
	ctgtypid::varchar(15) as ctgtypid,
	ctgtypdsc::varchar(50) as ctgtypdsc,
	dlvsts2::number(18,0) as dlvsts2,
	prdtaxamt::number(38,6) as prdtaxamt,
	mnfid::number(18,0) as mnfid,
	prdschdiscamt::number(38,6) as prdschdiscamt,
	prddbdiscamt::number(38,6) as prddbdiscamt,
	salwdsamt::number(38,6) as salwdsamt,
	discount::number(38,6) as discount,
	schid::number(18,0) as schid,
	createddate::timestamp_ntz(9) as createddate,
	fromdate::timestamp_ntz(9) as fromdate,
	todate::timestamp_ntz(9) as todate,
	netrate::number(38,6) as netrate,
	saleflag::varchar(3) as saleflag,
	weekno::number(18,0) as weekno,
	confirmsales::varchar(1) as confirmsales,
	subtotal4::number(21,3) as subtotal4,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
    from trans
)
select * from final