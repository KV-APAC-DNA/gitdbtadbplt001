with cir07dmprn_tbl as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIR07DMPRN_TBL
),
final as (
SELECT 
  cir07dmprn_tbl.dmprnno, 
  cir07dmprn_tbl.comment1, 
  cir07dmprn_tbl.kensu, 
  cir07dmprn_tbl.insertdate, 
  cir07dmprn_tbl.insertid, 
  cir07dmprn_tbl.deleteflg, 
  cir07dmprn_tbl.inserted_date, 
  cir07dmprn_tbl.inserted_by, 
  cir07dmprn_tbl.updated_date, 
  cir07dmprn_tbl.updated_by 
FROM 
  cir07dmprn_tbl
)
select * from final