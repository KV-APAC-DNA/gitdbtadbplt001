
with edw_vw_pop6_visits_prod_attribute_audits as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_VISITS_PROD_ATTRIBUTE_AUDITS 
),
edw_vw_pop6_store as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_STORE
),
edw_vw_pop6_salesperson as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_SALESPERSON
),
edw_vw_pop6_visits_display as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_VISITS_DISPLAY
),
final as (
SELECT 
  NULL AS salescampaignid, 
  prod.audit_form AS salescampaignname, 
  prod.audit_form AS salescampaigndescription, 
  prod.visit_date AS salescampaignstartdt, 
  to_date(prod.check_out_datetime) AS salescampaignenddt, 
  NULL AS targetgroupid, 
  NULL AS "target", 
  NULL AS customerid, 
  prod.pop_code AS cust_remotekey, 
  pop.pop_name AS customername, 
  pop.country, 
  pop.retail_environment_ps AS storetype, 
  pop.channel, 
  pop.sales_group_name AS salesgroup, 
  NULL AS soldtoparty, 
  prod.section_id AS mastertaskid, 
  prod.section AS text, 
  NULL AS "type", 
  NULL AS validfrom, 
  NULL AS validto, 
  NULL AS mastertaskstartdt, 
  NULL AS mastertaskenddt, 
  NULL AS taskid, 
  NULL AS salespersonid, 
  NULL AS surveyresponseid, 
  NULL AS taskstartdt, 
  to_date(prod.check_out_datetime) AS taskenddt, 
  NULL AS status, 
  prod.visit_id AS visitid, 
  NULL AS businessunitid, 
  NULL AS mastersurveyid, 
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
          ):: character varying
        ):: text || (':' :: character varying):: text
      ) || (
        (
          "date_part"(
            minute, 
            prod.check_out_datetime
          )
        ):: character varying
      ):: text
    )
  ):: character varying AS endtime, 
  prod.field_id AS questionkey, 
  prod.field_label AS questiontext, 
  NULL AS valuekey, 
  prod.response AS value, 
  prod.section AS mastersurveyname, 
  NULL AS mastersurveydescription, 
  NULL AS remotekey, 
  NULL AS title, 
  (
    (
      (
        (usr.first_name):: text || (' ' :: character varying):: text
      ) || (usr.last_name):: text
    )
  ):: character varying AS salespersonname, 
  'Y' AS flag 
FROM 
  (
    (
      edw_vw_pop6_visits_prod_attribute_audits prod 
      LEFT JOIN edw_vw_pop6_store pop ON (
        (
          trim(prod.popdb_id):: text = trim(pop.popdb_id):: text
        )
      )
    ) 
    LEFT JOIN edw_vw_pop6_salesperson usr ON (
      (
        trim(prod.username):: text = trim(usr.username):: text
      )
    )
  ) 
WHERE 
  (
    (
      trim(upper(
        (prod.field_type)):: text
      ) <> ('IMAGE' :: character varying):: text
    ) 
    AND (
      trim(upper(
        (prod.field_type)):: text
      ) <> ('PHOTO' :: character varying):: text
    )
  ) 
UNION ALL 
SELECT 
  NULL AS salescampaignid, 
  disp.display_name AS salescampaignname, 
  disp.display_name AS salescampaigndescription, 
  disp.start_date AS salescampaignstartdt, 
  disp.end_date AS salescampaignenddt, 
  NULL AS targetgroupid, 
  NULL AS "target", 
  NULL AS customerid, 
  disp.pop_code AS cust_remotekey, 
  pop.pop_name AS customername, 
  pop.country, 
  pop.retail_environment_ps AS storetype, 
  pop.channel, 
  pop.sales_group_name AS salesgroup, 
  NULL AS soldtoparty, 
  NULL AS mastertaskid, 
  disp.comments AS text, 
  NULL AS "type", 
  NULL AS validfrom, 
  NULL AS validto, 
  NULL AS mastertaskstartdt, 
  NULL AS mastertaskenddt, 
  NULL AS taskid, 
  NULL AS salespersonid, 
  NULL AS surveyresponseid, 
  NULL AS taskstartdt, 
  to_date(disp.check_out_datetime) AS taskenddt, 
  NULL AS status, 
  disp.visit_id AS visitid, 
  NULL AS businessunitid, 
  NULL AS mastersurveyid, 
  to_date(disp.check_out_datetime) AS enddate, 
  (
    (
      (
        (
          (
            "date_part"(
              hour, 
              disp.check_out_datetime
            )
          ):: character varying
        ):: text || (':' :: character varying):: text
      ) || (
        (
          "date_part"(
            minute, 
            disp.check_out_datetime
          )
        ):: character varying
      ):: text
    )
  ):: character varying AS endtime, 
  disp.field_id AS questionkey, 
  disp.field_label AS questiontext, 
  NULL AS valuekey, 
  CASE WHEN (
    (
      (disp.response):: text = ('0' :: character varying):: text
    ) 
    OR (
      (disp.response IS NULL) 
      AND ('0' IS NULL)
    )
  ) THEN 'NO' :: character varying WHEN (
    (
      (disp.response):: text = ('1' :: character varying):: text
    ) 
    OR (
      (disp.response IS NULL) 
      AND ('1' IS NULL)
    )
  ) THEN 'YES' :: character varying ELSE disp.response END AS value, 
  disp.comments AS mastersurveyname, 
  NULL AS mastersurveydescription, 
  NULL AS remotekey, 
  NULL AS title, 
  (
    (
      (
        (usr.first_name):: text || (' ' :: character varying):: text
      ) || (usr.last_name):: text
    )
  ):: character varying AS salespersonname, 
  'Y' AS flag 
FROM 
  (
    (
      edw_vw_pop6_visits_display disp 
      LEFT JOIN edw_vw_pop6_store pop ON (
        (
          trim((disp.popdb_id)):: text = trim((pop.popdb_id)):: text
        )
      )
    ) 
    LEFT JOIN edw_vw_pop6_salesperson usr ON (
      (
        trim((disp.username)):: text = trim((usr.username)):: text
      )
    )
  ) 
WHERE 
  (
    upper(
      trim((disp.field_type)):: text
    ) <> ('PHOTO' :: character varying):: text
  )
)
select * from final