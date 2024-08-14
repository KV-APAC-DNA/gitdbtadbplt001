WITH c_tbeccardcompanymst
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__c_tbeccardcompanymst')}}
    ),
final
AS (
    SELECT c_tbeccardcompanymst.c_dscardcompanyid AS code,
        COALESCE(c_tbeccardcompanymst.c_dscardcompanyname, 'その他'::CHARACTER VARYING) AS name,
        CASE 
            WHEN (
                    ((c_tbeccardcompanymst.c_dscardcompanyname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                    OR ((c_tbeccardcompanymst.c_dscardcompanyname IS NULL))
                    )
                THEN ('その他'::CHARACTER VARYING)::TEXT
            ELSE ((((c_tbeccardcompanymst.c_dscardcompanyid)::CHARACTER VARYING)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (c_tbeccardcompanymst.c_dscardcompanyname)::TEXT)
            END AS cname
    FROM c_tbeccardcompanymst
    )
SELECT *
FROM final
