WITH c_tbecpribuyitem
AS (
  SELECT *
  FROM DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECPRIBUYITEM
  ),
final
AS (
  SELECT c_tbecpribuyitem.diprivilegeid,
    c_tbecpribuyitem.diid,
    c_tbecpribuyitem.dikubun,
    c_tbecpribuyitem.dinum,
    c_tbecpribuyitem.diflg,
    c_tbecpribuyitem.dsprep,
    c_tbecpribuyitem.dsren,
    c_tbecpribuyitem.dselim,
    c_tbecpribuyitem.diprepusr,
    c_tbecpribuyitem.direnusr,
    c_tbecpribuyitem.dielimusr,
    c_tbecpribuyitem.dielimflg
  FROM c_tbecpribuyitem
  )
SELECT *
FROM final
