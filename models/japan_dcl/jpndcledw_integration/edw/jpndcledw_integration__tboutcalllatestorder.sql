with tbOutcallResult_wk as
(
select * from {{ ref('jpndcledw_integration__tboutcallresult_wk') }}
),
tbecorder as (

select * from {{ ref('jpndclitg_integration__tbecorder') }}
) ,
 tbEcSalesRouteMst as (
select * from {{ ref('jpndclitg_integration__tbecsalesroutemst') }}
),
C_TBECINQUIRE as 
(
select * from {{ ref('jpndclitg_integration__c_tbecinquire') }}
),
transformed as (
SELECT outcall_result.diUsrId,
       odr.diOrderID,
       sales_rt.dsroutename,
       MAX(rtrn.diinquireid) AS diinquireid
  FROM tbOutcallResult_wk outcall_result
 INNER JOIN tbecorder odr
    ON outcall_result.diUsrId = odr.diEcUsrID
 INNER JOIN (
             SELECT outcall_result.diUsrId,
                    MAX(diOrderID) AS MAX_orderID
               FROM tbOutcallResult_wk outcall_result
              INNER JOIN tbecorder odr
                 ON outcall_result.diUsrId = odr.diEcUsrID
              WHERE odr.diCancel = '0'
                AND odr.dielimflg = '0'
              GROUP BY outcall_result.diUsrId
            )latest_odr
    ON odr.diEcUsrID = latest_odr.diUsrId
   AND odr.diOrderID = latest_odr.MAX_orderID
  LEFT OUTER JOIN tbEcSalesRouteMst sales_rt
    ON odr.dirouteid = sales_rt.dirouteid
  LEFT OUTER JOIN c_tbEcInquire rtrn
    ON odr.diOrderID = rtrn.diorderid
   AND rtrn.diElimFlg = '0'
  GROUP BY outcall_result.diUsrId,odr.diOrderID,sales_rt.dsroutename
)
,final as (
select 
DIUSRID::NUMBER(38,0)  as DIUSRID,
DIORDERID::VARCHAR(30) as DIORDERID,
DSROUTENAME::VARCHAR(32) as DSROUTENAME,
DIINQUIREID::NUMBER(38,0) as DIINQUIREID
 from transformed
 )
 select * from final 