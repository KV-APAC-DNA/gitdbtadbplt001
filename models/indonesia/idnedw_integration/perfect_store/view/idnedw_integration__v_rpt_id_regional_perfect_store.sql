with edw_rpt_id_perfect_store as 
(
    select * from {{ ref('idnedw_integration__edw_rpt_id_perfect_store') }}
),
final as
(   
    SELECT 
        DISTINCT upper((edw_rpt_id_perfect_store.dataset)::text) AS dataset,
        NULL::text AS merchandisingresponseid,
        NULL::text AS surveyresponseid,
        edw_rpt_id_perfect_store.customerid,
        edw_rpt_id_perfect_store.salespersonid,
        NULL::text AS visitid,
        (edw_rpt_id_perfect_store.mrch_resp_startdt)::date AS mrch_resp_startdt,
        (edw_rpt_id_perfect_store.mrch_resp_enddt)::date AS mrch_resp_enddt,
        NULL::text AS mrch_resp_status,
        NULL::text AS mastersurveyid,
        NULL::text AS survey_status,
        edw_rpt_id_perfect_store.survey_enddate,
        NULL::text AS questionkey,
        edw_rpt_id_perfect_store.questiontext,
        NULL::text AS valuekey,
        edw_rpt_id_perfect_store.value,
        NULL::text AS productid,
        upper((edw_rpt_id_perfect_store.mustcarryitem)::text) AS mustcarryitem,
        edw_rpt_id_perfect_store.answerscore,
        upper((edw_rpt_id_perfect_store.presence)::text) AS presence,
        edw_rpt_id_perfect_store.outofstock,
        NULL::text AS mastersurveyname,
        upper((edw_rpt_id_perfect_store.kpi)::text) AS kpi,
        CASE
            WHEN (
                (
                    (
                        (
                            upper((edw_rpt_id_perfect_store.kpi)::text) = 'DISPLAY COMPLIANCE'::text
                        )
                        OR (
                            upper((edw_rpt_id_perfect_store.kpi)::text) = 'PLANOGRAM COMPLIANCE'::text
                        )
                    )
                    OR (
                        upper((edw_rpt_id_perfect_store.kpi)::text) = 'PROMO COMPLIANCE'::text
                    )
                )
                OR (
                    upper((edw_rpt_id_perfect_store.kpi)::text) = 'SHARE OF SHELF'::text
                )
            ) THEN edw_rpt_id_perfect_store.franchise
            ELSE NULL::character varying
        END AS category,
        CASE
            WHEN (
                (
                    (
                        (
                            upper((edw_rpt_id_perfect_store.kpi)::text) = 'DISPLAY COMPLIANCE'::text
                        )
                        OR (
                            upper((edw_rpt_id_perfect_store.kpi)::text) = 'PLANOGRAM COMPLIANCE'::text
                        )
                    )
                    OR (
                        upper((edw_rpt_id_perfect_store.kpi)::text) = 'PROMO COMPLIANCE'::text
                    )
                )
                OR (
                    upper((edw_rpt_id_perfect_store.kpi)::text) = 'SHARE OF SHELF'::text
                )
            ) THEN edw_rpt_id_perfect_store.brand
            ELSE NULL::character varying
        END AS segment,
        NULL::text AS vst_visitid,
        edw_rpt_id_perfect_store.scheduleddate,
        NULL::text AS scheduledtime,
        NULL::text AS duration,
        upper((edw_rpt_id_perfect_store.vst_status)::text) AS vst_status,
        edw_rpt_id_perfect_store.fisc_yr,
        (edw_rpt_id_perfect_store.fisc_per)::integer AS fisc_per,
        edw_rpt_id_perfect_store.firstname,
        edw_rpt_id_perfect_store.lastname,
        NULL::text AS cust_remotekey,
        edw_rpt_id_perfect_store.customername,
        edw_rpt_id_perfect_store.country,
        edw_rpt_id_perfect_store.state,
        NULL::text AS county,
        NULL::text AS district,
        edw_rpt_id_perfect_store.city,
        edw_rpt_id_perfect_store.storereference,
        edw_rpt_id_perfect_store.storetype,
        edw_rpt_id_perfect_store.channel,
        edw_rpt_id_perfect_store.salesgroup,
        NULL::text AS soldtoparty,
        NULL::text AS brand,
        NULL::text AS productname,
        NULL::text AS eannumber,
        NULL::text AS matl_num,
        NULL::text AS prod_hier_l1,
        NULL::text AS prod_hier_l2,
        CASE
            WHEN (
                (
                    upper((edw_rpt_id_perfect_store.kpi)::text) = 'MSL COMPLIANCE'::text
                )
                OR (
                    upper((edw_rpt_id_perfect_store.kpi)::text) = 'OOS COMPLIANCE'::text
                )
            ) THEN edw_rpt_id_perfect_store.franchise
            ELSE NULL::character varying
        END AS prod_hier_l3,
        CASE
            WHEN (
                (
                    upper((edw_rpt_id_perfect_store.kpi)::text) = 'MSL COMPLIANCE'::text
                )
                OR (
                    upper((edw_rpt_id_perfect_store.kpi)::text) = 'OOS COMPLIANCE'::text
                )
            ) THEN edw_rpt_id_perfect_store.brand
            ELSE NULL::character varying
        END AS prod_hier_l4,
        CASE
            WHEN (
                (
                    upper((edw_rpt_id_perfect_store.kpi)::text) = 'MSL COMPLIANCE'::text
                )
                OR (
                    upper((edw_rpt_id_perfect_store.kpi)::text) = 'OOS COMPLIANCE'::text
                )
            ) THEN edw_rpt_id_perfect_store.variant
            ELSE NULL::character varying
        END AS prod_hier_l5,
        CASE
            WHEN (
                (
                    upper((edw_rpt_id_perfect_store.kpi)::text) = 'MSL COMPLIANCE'::text
                )
                OR (
                    upper((edw_rpt_id_perfect_store.kpi)::text) = 'OOS COMPLIANCE'::text
                )
            ) THEN edw_rpt_id_perfect_store.putup
            ELSE NULL::character varying
        END AS prod_hier_l6,
        NULL::text AS prod_hier_l7,
        NULL::text AS prod_hier_l8,
        NULL::text AS prod_hier_l9,
        edw_rpt_id_perfect_store.kpi_chnl_wt,
        edw_rpt_id_perfect_store.mkt_share,
        upper((edw_rpt_id_perfect_store.ques_desc)::text) AS ques_desc,
        edw_rpt_id_perfect_store."y/n_flag",
        edw_rpt_id_perfect_store.posm_execution_flag,
        edw_rpt_id_perfect_store.rej_reason,
        NULL::text AS response,
        NULL::text AS response_score,
        ''::text AS acc_rej_reason,
        NULL::text AS actual,
        NULL::text AS "target",
        edw_rpt_id_perfect_store.priority_store_flag
    FROM edw_rpt_id_perfect_store
)
select * from final