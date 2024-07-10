
with v_slsc_tgtg_items_customer as (
select * from {{ ref('aspedw_integration__v_slsc_tgtg_items_customer' ) }}
),
v_slsc_mastertasks as (
select * from {{ ref('aspitg_integration__v_slsc_mastertasks') }}
),
v_tsk_task as (
select * from {{ ref('aspitg_integration__v_tsk_task') }}
),
v_bi_survey_response_values as (
select * from {{ ref('aspedw_integration__v_bi_survey_response_values' ) }}
),
v_ms_mastersurvey as (
select * from {{ ref('aspitg_integration__v_ms_mastersurvey') }}
),
v_slsp_salesperson as (
select * from {{ ref('aspitg_integration__v_slsp_salesperson') }}
),
final as (SELECT 
  slsc_customer.salescampaignid, 
  slsc_customer.salescampaignname, 
  slsc_customer.salescampaigndescription, 
  slsc_customer.salescampaignstartdt, 
  slsc_customer.salescampaignenddt, 
  slsc_customer.targetgroupid, 
  SLSC_CUSTOMER.target, 
  slsc_customer.customerid, 
  mst_task.mastertaskid, 
  mst_task.text, 
  mst_task.type, 
  mst_task.validfrom, 
  mst_task.validto, 
  mst_task.mastertaskstartdt, 
  mst_task.mastertaskenddt, 
  task.taskid, 
  task.salespersonid, 
  task.surveyresponseid, 
  task.taskstartdt, 
  task.taskenddt, 
  task.status, 
  task.visitid, 
  sr_resp.businessunitid, 
  sr_resp.mastersurveyid, 
  sr_resp.enddate, 
  sr_resp.endtime, 
  sr_resp.questionkey, 
  sr_resp.questiontext, 
  sr_resp.valuekey, 
  sr_resp.value, 
  ms_survey.mastersurveyname, 
  ms_survey.mastersurveydescription, 
  salesperson.remotekey, 
  salesperson.title, 
  salesperson.salespersonname 
FROM 
  (
    (
      (
        (
          (
            (
              SELECT 
                v_slsc_tgtg_items_customer.salescampaignid, 
                v_slsc_tgtg_items_customer.salescampaignname, 
                v_slsc_tgtg_items_customer.salescampaigndescription, 
                v_slsc_tgtg_items_customer.salescampaignstartdt, 
                v_slsc_tgtg_items_customer.salescampaignenddt, 
                v_slsc_tgtg_items_customer.targetgroupid, 
                V_SLSC_TGTG_ITEMS_CUSTOMER."target", 
                v_slsc_tgtg_items_customer.customerid 
              FROM 
                v_slsc_tgtg_items_customer
            ) slsc_customer 
            LEFT JOIN (
              SELECT 
                DISTINCT v_slsc_mastertasks.mastertaskid, 
                v_slsc_mastertasks.salescampaignid, 
                v_slsc_mastertasks.text, 
                v_slsc_mastertasks.type, 
                v_slsc_mastertasks.validfrom, 
                v_slsc_mastertasks.validto, 
                v_slsc_mastertasks.startdate AS mastertaskstartdt, 
                v_slsc_mastertasks.enddate AS mastertaskenddt 
              FROM 
                v_slsc_mastertasks 
              WHERE 
                (v_slsc_mastertasks.rank = 1)
            ) mst_task ON (
              (
                (slsc_customer.salescampaignid):: text = (mst_task.salescampaignid):: text
              )
            )
          ) 
          LEFT JOIN (
            SELECT 
              DISTINCT v_tsk_task.taskid, 
              v_tsk_task.businessunitid, 
              v_tsk_task.mastertaskid, 
              v_tsk_task.customerid, 
              v_tsk_task.salespersonid, 
              v_tsk_task.salescampaignid, 
              v_tsk_task.surveyresponseid, 
              v_tsk_task.startdate AS taskstartdt, 
              v_tsk_task.enddate AS taskenddt, 
              v_tsk_task.status, 
              v_tsk_task.visitid 
            FROM 
              v_tsk_task 
            WHERE 
              (v_tsk_task.rank = 1)
          ) task ON (
            (
              (
                (
                  (
                    (mst_task.mastertaskid):: text = (task.mastertaskid):: text
                  ) 
                  AND (
                    (mst_task.salescampaignid):: text = (task.salescampaignid):: text
                  )
                ) 
                AND (
                  (slsc_customer.customerid):: text = (task.customerid):: text
                )
              ) 
              AND (
                (slsc_customer.salescampaignid):: text = (task.salescampaignid):: text
              )
            )
          )
        ) 
        LEFT JOIN (
          SELECT 
            v_bi_survey_response_values.surveyresponseid, 
            v_bi_survey_response_values.businessunitid, 
            v_bi_survey_response_values.customerid, 
            v_bi_survey_response_values.salespersonid, 
            v_bi_survey_response_values.salescampaignid, 
            v_bi_survey_response_values.visitid, 
            v_bi_survey_response_values.taskid, 
            v_bi_survey_response_values.mastertaskid, 
            v_bi_survey_response_values.mastersurveyid, 
            v_bi_survey_response_values.status, 
            v_bi_survey_response_values.enddate, 
            v_bi_survey_response_values.endtime, 
            v_bi_survey_response_values.questionkey, 
            v_bi_survey_response_values.questiontext, 
            v_bi_survey_response_values.valuekey, 
            v_bi_survey_response_values.value 
          FROM 
            v_bi_survey_response_values
        ) sr_resp ON (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (task.customerid):: text = (sr_resp.customerid):: text
                          ) 
                          AND (
                            (task.salespersonid):: text = (sr_resp.salespersonid):: text
                          )
                        ) 
                        AND (
                          (task.salescampaignid):: text = (sr_resp.salescampaignid):: text
                        )
                      ) 
                      AND (
                        (task.surveyresponseid):: text = (sr_resp.surveyresponseid):: text
                      )
                    ) 
                    AND (
                      (task.visitid):: text = (sr_resp.visitid):: text
                    )
                  ) 
                  AND (
                    (task.businessunitid):: text = (sr_resp.businessunitid):: text
                  )
                ) 
                AND (
                  (slsc_customer.customerid):: text = (sr_resp.customerid):: text
                )
              ) 
              AND (
                (mst_task.salescampaignid):: text = (sr_resp.salescampaignid):: text
              )
            ) 
            AND (
              (slsc_customer.salescampaignid):: text = (sr_resp.salescampaignid):: text
            )
          )
        )
      ) 
      LEFT JOIN (
        SELECT 
          v_ms_mastersurvey.mastersurveyid, 
          v_ms_mastersurvey.mastersurveyname, 
          v_ms_mastersurvey.mastersurveydescription 
        FROM 
          v_ms_mastersurvey 
        WHERE 
          (v_ms_mastersurvey.rank = 1)
      ) ms_survey ON (
        (
          (sr_resp.mastersurveyid):: text = (ms_survey.mastersurveyid):: text
        )
      )
    ) 
    LEFT JOIN (
      SELECT 
        v_slsp_salesperson.salespersonid, 
        v_slsp_salesperson.remotekey, 
        v_slsp_salesperson.title, 
        (
          (
            (v_slsp_salesperson.firstname):: text || (' ' :: character varying):: text
          ) || (v_slsp_salesperson.lastname):: text
        ) AS salespersonname 
      FROM 
        v_slsp_salesperson 
      WHERE 
        (v_slsp_salesperson.rank = 1)
    ) salesperson ON (
      (
        (task.salespersonid):: text = (salesperson.salespersonid):: text
      )
    )
  )
)
select * from final
