WITH c_tbecpaymentpatternmst
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__c_tbecpaymentpatternmst') }}
    ),
FINAL
AS (
    SELECT c_tbecpaymentpatternmst.c_dspaymentptnkbn AS code,
        COALESCE(c_tbecpaymentpatternmst.c_dspaymentlongname, 'その他'::CHARACTER VARYING) AS name,
        CASE 
            WHEN (
                    ((c_tbecpaymentpatternmst.c_dspaymentlongname)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                    OR (
                        (c_tbecpaymentpatternmst.c_dspaymentlongname IS NULL)
                        )
                    )
                THEN ('その他'::CHARACTER VARYING)::TEXT
            ELSE (((c_tbecpaymentpatternmst.c_dspaymentptnkbn)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (c_tbecpaymentpatternmst.c_dspaymentlongname)::TEXT)
            END AS cname
    FROM c_tbecpaymentpatternmst c_tbecpaymentpatternmst
    )
SELECT *
FROM

FINAL
