with edw_vw_pop6_visits_prod_attribute_audits as (
    select * from ntaedw_integration.edw_vw_pop6_visits_prod_attribute_audits
),
edw_vw_pop6_store as (
    select * from ntaedw_integration.edw_vw_pop6_store
),
edw_vw_pop6_salesperson as (
    select * from ntaedw_integration.edw_vw_pop6_salesperson
),
edw_vw_pop6_visits_display as (
    select * from ntaedw_integration.edw_vw_pop6_visits_display
),
union_1 as
(   
    SELECT 
        null AS salescampaignid,
        prod.audit_form AS salescampaignname,
        prod.audit_form AS salescampaigndescription,
        prod.visit_date AS salescampaignstartdt,
        to_date(prod.check_out_datetime) AS salescampaignenddt,
        null AS targetgroupid,
        null AS "target",
        null AS customerid,
        prod.pop_code AS cust_remotekey,
        pop.pop_name AS customername,
        pop.country,
        pop.retail_environment_ps AS storetype,
        pop.channel,
        pop.sales_group_name AS salesgroup,
        null AS soldtoparty,
        prod.section_id AS mastertaskid,
        prod.section AS text,
        null AS "type",
        null AS validfrom,
        null AS validto,
        null AS mastertaskstartdt,
        null AS mastertaskenddt,
        null AS taskid,
        null AS salespersonid,
        null AS surveyresponseid,
        null AS taskstartdt,
        to_date(prod.check_out_datetime) AS taskenddt,
        null AS status,
        prod.visit_id AS visitid,
        null AS businessunitid,
        null AS mastersurveyid,
        to_date(prod.check_out_datetime) AS enddate,
        (
            (
                (
                    (
                        (
                            "date_part"(
                                hour,
                                prod.check_out_datetime
                            )
                        )::character varying
                    )::text || (':'::character varying)::text
                ) || (
                    (
                        "date_part"(
                            min,
                            prod.check_out_datetime
                        )
                    )::character varying
                )::text
            )
        )::character varying AS endtime,
        prod.field_id AS questionkey,
        prod.field_label AS questiontext,
        null AS valuekey,
        prod.response AS value,
        prod.section AS mastersurveyname,
        null AS mastersurveydescription,
        null AS remotekey,
        null AS title,
        (
            (
                (
                    (usr.first_name)::text || (' '::character varying)::text
                ) || (usr.last_name)::text
            )
        )::character varying AS salespersonname,
        'Y' AS flag
    FROM
        edw_vw_pop6_visits_prod_attribute_audits prod
        LEFT JOIN edw_vw_pop6_store pop ON ((rtrim(prod.popdb_id)::text = rtrim(pop.popdb_id)::text))
        LEFT JOIN edw_vw_pop6_salesperson usr ON ((rtrim(prod.username)::text = rtrim(usr.username)::text))
    WHERE 
        rtrim(upper((prod.field_type)::text)) <> ('IMAGE'::character varying)::text
        AND rtrim(upper((prod.field_type)::text)) <> ('PHOTO'::character varying)::text
),
union_2 as
(
    SELECT 
        null AS salescampaignid,
        disp.display_name AS salescampaignname,
        disp.display_name AS salescampaigndescription,
        disp.start_date AS salescampaignstartdt,
        disp.end_date AS salescampaignenddt,
        null AS targetgroupid,
        null AS "target",
        null AS customerid,
        disp.pop_code AS cust_remotekey,
        pop.pop_name AS customername,
        pop.country,
        pop.retail_environment_ps AS storetype,
        pop.channel,
        pop.sales_group_name AS salesgroup,
        null AS soldtoparty,
        null AS mastertaskid,
        disp.comments AS text,
        null AS "type",
        null AS validfrom,
        null AS validto,
        null AS mastertaskstartdt,
        null AS mastertaskenddt,
        null AS taskid,
        null AS salespersonid,
        null AS surveyresponseid,
        null AS taskstartdt,
        to_date(disp.check_out_datetime) AS taskenddt,
        null AS status,
        disp.visit_id AS visitid,
        null AS businessunitid,
        null AS mastersurveyid,
        to_date(disp.check_out_datetime) AS enddate,
        (
            (
                (
                    (
                        (
                            date_part(
                                hour,
                                disp.check_out_datetime
                            )
                        )::character varying
                    )::text || (':'::character varying)::text
                ) || (
                    (
                        date_part(
                            min,
                            disp.check_out_datetime
                        )
                    )::character varying
                )::text
            )
        )::character varying AS endtime,
        disp.field_id AS questionkey,
        disp.field_label AS questiontext,
        null AS valuekey,
        CASE
            WHEN (
                (
                    (disp.response)::text = ('0'::character varying)::text
                )
                OR (
                    (disp.response IS NULL)
                    AND ('0' IS NULL)
                )
            ) THEN 'NO'::character varying
            WHEN (
                (
                    (disp.response)::text = ('1'::character varying)::text
                )
                OR (
                    (disp.response IS NULL)
                    AND ('1' IS NULL)
                )
            ) THEN 'YES'::character varying
            ELSE disp.response
        END AS value,
        disp.comments AS mastersurveyname,
        null AS mastersurveydescription,
        null AS remotekey,
        null AS title,
        (
            (
                (
                    (usr.first_name)::text || (' '::character varying)::text
                ) || (usr.last_name)::text
            )
        )::character varying AS salespersonname,
        'Y' AS flag
    FROM edw_vw_pop6_visits_display disp
        LEFT JOIN edw_vw_pop6_store pop ON ((rtrim(disp.popdb_id)::text = rtrim(pop.popdb_id)::text))
        LEFT JOIN edw_vw_pop6_salesperson usr ON ((rtrim(disp.username)::text = rtrim(usr.username)::text))
    WHERE rtrim(upper((disp.field_type)::text)) <> ('PHOTO'::character varying)::text
),
final as
(
    select * from union_1
    union all
    select * from union_2
)
select * from final