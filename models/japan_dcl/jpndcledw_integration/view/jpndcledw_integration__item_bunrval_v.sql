with hanyo_attr as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.HANYO_ATTR),
final as (
SELECT 
  (hanyo_attr.attr1):: character varying(1) AS hin_bunr_taik_id, 
  (hanyo_attr.attr2):: character varying(1) AS hin_bunr_kaisou_kbn, 
  (hanyo_attr.attr3):: character varying(6) AS hin_bunr_val_cd, 
  (hanyo_attr.attr4):: character varying(100) AS hin_bunr_val_nms, 
  (hanyo_attr.attr5):: character varying(6) AS j_kaisou_hinmk_bunr_cd 
FROM 
  hanyo_attr 
WHERE 
  (
    (hanyo_attr.kbnmei):: text = ('BUNRVAL' :: character varying):: text
  )
)
select * from final
