WITH pm
AS (
  SELECT *
  FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.EDW_MDS_JP_DCL_PRODUCT_MASTER
  ),
cim03
AS (
  SELECT *
  FROM DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.CIM03ITEM_ZAIKO
  ),
final
AS (
  SELECT pm.itemcode,
    pm.attr06 AS itembunrui,
    NULL AS bk_itemcode
  FROM (
     pm JOIN cim03 ON (((RTRIM(pm.itemcode))::TEXT = (RTRIM(cim03.itemcode))::TEXT))
    )
  )
SELECT *
FROM final