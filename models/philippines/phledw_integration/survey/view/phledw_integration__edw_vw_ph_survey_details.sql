WITH itg_ph_tbl_surveyanswers
AS (
    SELECT *
    FROM {{ ref('phlitg_integration__itg_ph_tbl_surveyanswers') }}
    ),
itg_ph_tbl_surveycustomers
AS (
    SELECT *
    FROM {{ ref('phlitg_integration__itg_ph_tbl_surveycustomers') }}
    ),
itg_ph_tbl_acctexec
AS (
    SELECT *
    FROM {{ ref('phlitg_integration__itg_ph_tbl_acctexec') }}
    ),
itg_ph_tbl_surveychoices
AS (
    SELECT *
    FROM {{ ref('phlitg_integration__itg_ph_tbl_surveychoices') }}
    ),
itg_ph_tbl_surveyisequestion
AS (
    SELECT *
    FROM {{ ref('phlitg_integration__itg_ph_tbl_surveyisequestion') }}
    ),
itg_ph_tbl_surveyisehdr
AS (
    SELECT *
    FROM {{ ref('phlitg_integration__itg_ph_tbl_surveyisehdr') }}
    ),
itg_ph_tbl_surveynotes
AS (
    SELECT *
    FROM {{ ref('phlitg_integration__itg_ph_tbl_surveynotes') }}
    ),
srv_ans
AS (
    SELECT derived_table1.custcode,
        derived_table1.slsperid,
        derived_table1.branchcode,
        derived_table1.iseid,
        derived_table1.quesno,
        derived_table1.answerseq,
        derived_table1.answervalue,
        derived_table1.answerscore,
        derived_table1.oos,
        derived_table1.createddate,
        derived_table1.filename_dt,
        derived_table1.rnk
    FROM (
        SELECT DISTINCT rtrim(itg_ph_tbl_surveyanswers.custcode) as custcode,
            rtrim(itg_ph_tbl_surveyanswers.slsperid) as slsperid,
            rtrim(itg_ph_tbl_surveyanswers.branchcode) as branchcode,
            rtrim(itg_ph_tbl_surveyanswers.iseid) as iseid,
            rtrim(itg_ph_tbl_surveyanswers.quesno) as quesno,
            rtrim(itg_ph_tbl_surveyanswers.answerseq) as answerseq,
            rtrim(itg_ph_tbl_surveyanswers.answervalue) as answervalue,
            rtrim(itg_ph_tbl_surveyanswers.answerscore) as answerscore,
            rtrim(itg_ph_tbl_surveyanswers.oos) as oos,
            to_date(itg_ph_tbl_surveyanswers.createddate) AS createddate,
            rtrim(itg_ph_tbl_surveyanswers.filename_dt) as filename_dt,
            row_number() OVER (
                PARTITION BY rtrim(itg_ph_tbl_surveyanswers.custcode),
                rtrim(itg_ph_tbl_surveyanswers.slsperid),
                rtrim(itg_ph_tbl_surveyanswers.branchcode),
                rtrim(itg_ph_tbl_surveyanswers.iseid),
                rtrim(itg_ph_tbl_surveyanswers.quesno),
                rtrim(itg_ph_tbl_surveyanswers.answerseq),
                rtrim(itg_ph_tbl_surveyanswers.createddate) ORDER BY itg_ph_tbl_surveyanswers.createddate,
                    itg_ph_tbl_surveyanswers.filename_dt DESC
                ) AS rnk
        FROM itg_ph_tbl_surveyanswers
        ) derived_table1
    WHERE (derived_table1.rnk = 1)
    ),
srv_cust
AS (
    SELECT derived_table2.custcode,
        derived_table2.slsperid,
        derived_table2.branchcode,
        derived_table2.iseid,
        derived_table2.visitdate,
        derived_table2.storeprioritization,
        derived_table2.filename_dt,
        derived_table2.rnk
    FROM (
        SELECT DISTINCT itg_ph_tbl_surveycustomers.custcode,
            itg_ph_tbl_surveycustomers.slsperid,
            itg_ph_tbl_surveycustomers.branchcode,
            itg_ph_tbl_surveycustomers.iseid,
            to_date(itg_ph_tbl_surveycustomers.visitdate) AS visitdate,
            itg_ph_tbl_surveycustomers.storeprioritization,
            itg_ph_tbl_surveycustomers.filename_dt,
            row_number() OVER (
                PARTITION BY itg_ph_tbl_surveycustomers.custcode,
                itg_ph_tbl_surveycustomers.slsperid,
                itg_ph_tbl_surveycustomers.branchcode,
                itg_ph_tbl_surveycustomers.iseid,
                itg_ph_tbl_surveycustomers.visitdate ORDER BY itg_ph_tbl_surveycustomers.visitdate,
                    itg_ph_tbl_surveycustomers.filename_dt DESC
                ) AS rnk
        FROM itg_ph_tbl_surveycustomers
        ) derived_table2
    WHERE (derived_table2.rnk = 1)
    ),
srv_choices
AS (
    SELECT derived_table3.iseid,
        derived_table3.quesno,
        derived_table3.answerseq,
        derived_table3.putupid,
        derived_table3.putupdesc,
        derived_table3.brandid,
        derived_table3.brand,
        derived_table3.filename_dt,
        derived_table3.rnk
    FROM (
        SELECT DISTINCT itg_ph_tbl_surveychoices.iseid,
            itg_ph_tbl_surveychoices.quesno,
            itg_ph_tbl_surveychoices.answerseq,
            itg_ph_tbl_surveychoices.putupid,
            itg_ph_tbl_surveychoices.putupdesc,
            itg_ph_tbl_surveychoices.brandid,
            itg_ph_tbl_surveychoices.brand,
            itg_ph_tbl_surveychoices.filename_dt,
            row_number() OVER (
                PARTITION BY itg_ph_tbl_surveychoices.iseid,
                itg_ph_tbl_surveychoices.quesno,
                itg_ph_tbl_surveychoices.answerseq ORDER BY itg_ph_tbl_surveychoices.filename_dt DESC
                ) AS rnk
        FROM itg_ph_tbl_surveychoices
        ) derived_table3
    WHERE (derived_table3.rnk = 1)
    ),
srv_ques
AS (
    SELECT derived_table4.iseid,
        derived_table4.quesno,
        derived_table4.quesclasscode,
        derived_table4.quesclassdesc,
        derived_table4.totalscore,
        derived_table4.franchisecode,
        derived_table4.filename_dt,
        derived_table4.rnk
    FROM (
        SELECT DISTINCT itg_ph_tbl_surveyisequestion.iseid,
            itg_ph_tbl_surveyisequestion.quesno,
            trim(ltrim((itg_ph_tbl_surveyisequestion.quesclasscode)::TEXT, ('0'::CHARACTER VARYING)::TEXT)) AS quesclasscode,
            itg_ph_tbl_surveyisequestion.quesclassdesc,
            itg_ph_tbl_surveyisequestion.totalscore,
            itg_ph_tbl_surveyisequestion.franchisecode,
            itg_ph_tbl_surveyisequestion.filename_dt,
            row_number() OVER (
                PARTITION BY itg_ph_tbl_surveyisequestion.iseid,
                itg_ph_tbl_surveyisequestion.quesno ORDER BY itg_ph_tbl_surveyisequestion.filename_dt DESC
                ) AS rnk
        FROM itg_ph_tbl_surveyisequestion
        ) derived_table4
    WHERE (derived_table4.rnk = 1)
    ),
srv_notes
AS (
    SELECT derived_table6.iseid,
        derived_table6.slsperid,
        derived_table6.custcode,
        derived_table6.branchcode,
        derived_table6.createddate,
        derived_table6.questionno,
        derived_table6.answerseq,
        derived_table6.answernotes,
        derived_table6.filename_dt,
        derived_table6.rnk
    FROM (
        SELECT DISTINCT itg_ph_tbl_surveynotes.iseid,
            itg_ph_tbl_surveynotes.slsperid,
            itg_ph_tbl_surveynotes.custcode,
            itg_ph_tbl_surveynotes.branchcode,
            to_date(itg_ph_tbl_surveynotes.createddate) AS createddate,
            itg_ph_tbl_surveynotes.questionno,
            itg_ph_tbl_surveynotes.answerseq,
            itg_ph_tbl_surveynotes.answernotes,
            itg_ph_tbl_surveynotes.filename_dt,
            --need to change this in j&j as well to match answernotes as we have to add all cols used in left join in row_no function
            row_number() OVER (
                PARTITION BY itg_ph_tbl_surveynotes.iseid,slsperid,custcode,branchcode,createddate,
                itg_ph_tbl_surveynotes.answernotes ORDER BY itg_ph_tbl_surveynotes.filename_dt DESC
                ) AS rnk
            -- row_number() OVER (
            --     PARTITION BY itg_ph_tbl_surveynotes.iseid,
            --     itg_ph_tbl_surveynotes.answernotes ORDER BY itg_ph_tbl_surveynotes.filename_dt DESC
            --     ) AS rnk
        FROM itg_ph_tbl_surveynotes
        ) derived_table6
    WHERE (derived_table6.rnk = 1)
    ),
srv_hdr AS (
    SELECT derived_table5.iseid,
        derived_table5.isedesc,
        derived_table5.startdate,
        derived_table5.enddate,
        derived_table5.filename_dt,
        derived_table5.rnk
    FROM (
        SELECT DISTINCT itg_ph_tbl_surveyisehdr.iseid,
            itg_ph_tbl_surveyisehdr.isedesc,
            to_date(itg_ph_tbl_surveyisehdr.startdate) AS startdate,
            to_date(itg_ph_tbl_surveyisehdr.enddate) AS enddate,
            itg_ph_tbl_surveyisehdr.filename_dt,
            row_number() OVER (
                PARTITION BY itg_ph_tbl_surveyisehdr.iseid ORDER BY itg_ph_tbl_surveyisehdr.filename_dt DESC
                ) AS rnk
        FROM itg_ph_tbl_surveyisehdr
        ) derived_table5
    WHERE (derived_table5.rnk = 1)
),
final AS (
    SELECT srv_ans.branchcode,
        srv_ans.slsperid,
        srv_ans.custcode,
        srv_ans.iseid,
        srv_ans.quesno AS ans_quesno,
        srv_ans.answerseq,
        srv_ans.answervalue,
        srv_ans.answerscore,
        srv_ans.oos,
        srv_ans.createddate,
        srv_cust.storeprioritization,
        acct_exec.name,
        srv_choices.putupid,
        srv_choices.putupdesc,
        srv_choices.brandid,
        srv_choices.brand,
        srv_ques.quesno,
        srv_ques.quesclasscode,
        srv_ques.quesclassdesc,
        srv_ques.totalscore,
        srv_ques.franchisecode,
        srv_hdr.isedesc,
        srv_hdr.startdate,
        srv_hdr.enddate,
        srv_notes.answernotes
    FROM
        srv_ans
        LEFT JOIN srv_cust ON 
        (
                rtrim(srv_ans.custcode)::TEXT = rtrim(srv_cust.custcode)::TEXT
            AND rtrim(srv_ans.slsperid)::TEXT = rtrim(srv_cust.slsperid)::TEXT
            AND rtrim(srv_ans.branchcode)::TEXT = rtrim(srv_cust.branchcode)::TEXT
            AND rtrim(srv_ans.iseid)::TEXT = rtrim(srv_cust.iseid)::TEXT
            AND srv_ans.createddate = srv_cust.visitdate
        )
        LEFT JOIN (
            SELECT DISTINCT itg_ph_tbl_acctexec.slsperid,
                itg_ph_tbl_acctexec.name
            FROM itg_ph_tbl_acctexec
        ) acct_exec ON (
            (
                rtrim(srv_ans.slsperid)::TEXT = rtrim(acct_exec.slsperid)::TEXT
            )
        )
        LEFT JOIN srv_choices ON (
            (
                (
                    (
                        rtrim(srv_ans.iseid)::TEXT = rtrim(srv_choices.iseid)::TEXT
                    )
                    AND rtrim(srv_ans.quesno) = rtrim(srv_choices.quesno)
                )
                AND rtrim(srv_ans.answerseq) = rtrim(srv_choices.answerseq)
            )
        )
        LEFT JOIN srv_ques ON (
            (
                (rtrim(srv_ans.iseid)::TEXT = rtrim(srv_ques.iseid)::TEXT)
                AND rtrim(srv_ans.quesno) = rtrim(srv_ques.quesno)
            )
        )
        LEFT JOIN srv_hdr ON ((rtrim(srv_ans.iseid)::TEXT = rtrim(srv_hdr.iseid)::TEXT))
        LEFT JOIN srv_notes ON 
        (
                rtrim(srv_ans.iseid)::TEXT = rtrim(srv_notes.iseid)::TEXT
            AND rtrim(srv_ans.slsperid)::TEXT = rtrim(srv_notes.slsperid)::TEXT
            AND rtrim(srv_ans.custcode)::TEXT = rtrim(srv_notes.custcode)::TEXT
            AND rtrim(srv_ans.branchcode)::TEXT = rtrim(srv_notes.branchcode)::TEXT
            AND rtrim(srv_ans.createddate) = rtrim(srv_notes.createddate)
            AND rtrim(srv_ans.quesno) = rtrim(srv_notes.questionno)
            AND rtrim(srv_ans.answerseq) = rtrim(srv_notes.answerseq)
        )
)
SELECT *
FROM final 