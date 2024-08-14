with c_tbtelcompanymst as(
    select * from {{ ref('jpndclitg_integration__c_tbtelcompanymst') }}
),
 zcmmnsoshitaik as(
select * from {{ source('jpdclitg_integration', 'zcmmnsoshitaik') }}
)
,hanyo_attr_bkp as 
(
    select * from {{ source('jpdcledw_integration', 'hanyo_attr_bkp') }}
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