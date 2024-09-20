with c_tbecusrcommentclassmst
as
(
        SELECT * FROM {{ ref('jpndclitg_integration__c_tbecusrcommentclassmst') }}
),
final as
(
    SELECT c_tbecusrcommentclassmst.c_dsusrcommentclasskbn AS code,
        c_tbecusrcommentclassmst.c_dsusrcommentclassname AS name,
        (((c_tbecusrcommentclassmst.c_dsusrcommentclasskbn)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (c_tbecusrcommentclassmst.c_dsusrcommentclassname)::TEXT) AS cname
    FROM  c_tbecusrcommentclassmst
)
SELECT * FROM final