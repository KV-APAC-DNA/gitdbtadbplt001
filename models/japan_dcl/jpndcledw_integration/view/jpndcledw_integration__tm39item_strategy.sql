WITH pm
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__edw_mds_jp_dcl_product_master') }}
  ),
cim03
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__cim03item_zaiko') }}
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