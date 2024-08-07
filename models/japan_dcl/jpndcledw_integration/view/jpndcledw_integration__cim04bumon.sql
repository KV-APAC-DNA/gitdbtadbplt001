with C_TBTELCOMPANYMST as(
    select * from SNAPJPDCLITG_INTEGRATION.C_TBTELCOMPANYMST
),
 ZCMMNSOSHITAIK as(
select * from SNAPJPDCLITG_INTEGRATION.ZCMMNSOSHITAIK
)
,HANYO_ATTR_BKP as 
(
    select * from SNAPJPDCLEDW_INTEGRATION.HANYO_ATTR_BKP
)
,final as(

SELECT c_tbtelcompanymst.c_dstelcompanycd AS bumoncode,
    c_tbtelcompanymst.c_dstelcompayname AS name,
    'NEXT' AS ciflg
FROM C_TBTELCOMPANYMST c_tbtelcompanymst

UNION ALL

SELECT zcmmnsoshitaik.bmn_naibukanri_no AS bumoncode,
    zcmmnsoshitaik.bmn_nms AS name,
    'PORT' AS ciflg
FROM ZCMMNSOSHITAIK zcmmnsoshitaik
WHERE (
        zcmmnsoshitaik.kaisha_cd IN (
            SELECT hanyo_attr.attr1
            FROM HANYO_ATTR_BKP hanyo_attr
            WHERE ((hanyo_attr.kbnmei)::TEXT = ('KAISYA'::CHARACTER VARYING)::TEXT)
            )
        )
)
select * from final