WITH v_sr_surveyresponse
AS (
    SELECT *
    FROM {{ ref('aspitg_integration__v_sr_surveyresponse') }}
    ),

v_sr_responses
AS (
    SELECT *
    FROM {{ ref('aspitg_integration__v_sr_responses') }}
    ),

v_sr_response_values
AS (
    SELECT *
    FROM {{ ref('aspitg_integration__v_sr_response_values') }}
    ),

resp
AS (
    SELECT r.surveyresponseid,
        r.questionkey,
        r.questiontext,
        rv.valuekey,
        rv.value
    FROM v_sr_responses r,
        v_sr_response_values rv
    WHERE (
            (
                (
                    ((r.surveyresponseid)::TEXT = (rv.surveyresponseid)::TEXT)
                    AND ((r.questionkey)::TEXT = (rv.questionkey)::TEXT)
                    )
                AND (r.rank = 1)
                )
            AND (rv.rank = 1)
            )
    
    UNION
    
    SELECT r1.surveyresponseid,
        r1.questionkey,
        r1.questiontext,
        COALESCE(r1.valuekey, 'NA'::CHARACTER VARYING) AS "coalesce",
        r1.value
    FROM v_sr_responses r1
    WHERE (
            (r1.rank = 1)
            AND (
                NOT (
                    EXISTS (
                        SELECT rv1.surveyresponseid
                        FROM v_sr_response_values rv1
                        WHERE (
                                (
                                    ((rv1.surveyresponseid)::TEXT = (r1.surveyresponseid)::TEXT)
                                    AND ((rv1.questionkey)::TEXT = (r1.questionkey)::TEXT)
                                    )
                                AND (rv1.rank = 1)
                                )
                        )
                    )
                )
            )
    ),
final
AS (
    SELECT sr.surveyresponseid,
        sr.businessunitid,
        sr.customerid,
        sr.salespersonid,
        sr.salescampaignid,
        sr.visitid,
        sr.taskid,
        sr.mastertaskid,
        sr.mastersurveyid,
        sr.STATUS,
        sr.enddate,
        sr.endtime,
        resp.questionkey,
        resp.questiontext,
        resp.valuekey,
        resp.value
    FROM v_sr_surveyresponse sr,
        resp
    WHERE (
            (sr.rank = 1)
            AND ((sr.surveyresponseid)::TEXT = (resp.surveyresponseid)::TEXT)
            )
    )
SELECT *
FROM final