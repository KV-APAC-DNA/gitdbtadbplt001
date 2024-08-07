with c_tbecinquire as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECINQUIRE
),
tbecordermeisai as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECORDERMEISAI
),
c_tbecinquiremeisai as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECINQUIREMEISAI
),
final as (
SELECT 
  c_tbecinquiremeisai.diinquireid AS odrreturnno, 
  c_tbecinquiremeisai.dimeisaiid AS odrreturnitemseqno, 
  c_tbecinquiremeisai.dsitemid AS itemcode, 
  c_tbecinquiremeisai.c_dihenpinnum AS prequantity, 
  c_tbecinquiremeisai.c_dihenpinnum AS quantity, 
  tbecordermeisai.dimeisaiid AS returnsourceno, 
  c_tbecinquire.c_dihenpinkakuteidt AS returndate, 
  c_tbecinquiremeisai.dihenpinriyuid AS returntype, 
  c_tbecinquiremeisai.dielimflg AS deleteflag, 
  c_tbecinquiremeisai.dielimflg AS cancelflag, 
  tbecordermeisai.ditotalprc AS priceinctax 
FROM 
  (
    (
      c_tbecinquiremeisai 
      JOIN c_tbecinquire ON (
        (
          c_tbecinquiremeisai.diinquireid = c_tbecinquire.diinquireid
        )
      )
    ) 
    JOIN tbecordermeisai ON (
      (
        tbecordermeisai.dimeisaiid = c_tbecinquiremeisai.diordermeisaiid
      )
    )
  )
)
select * from final