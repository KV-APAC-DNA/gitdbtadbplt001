with edw_vw_pop6_visits_sku_audits as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_VISITS_SKU_AUDITS
),
edw_vw_pop6_products as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_PRODUCTS
),
edw_vw_pop6_salesperson as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_SALESPERSON
),
EDW_VW_POP6_STORE as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_STORE 
),
final as (SELECT 
  vst.popdb_id AS customerid, 
  cust.pop_name AS customername, 
  cust.pop_code AS remotekey, 
  cust.country, 
  cust.customer AS storetype, 
  cust.channel, 
  cust.sales_group_name AS salesgroup, 
  vst.visit_id AS visitid, 
  to_char(
    vst.check_in_datetime, 
    ('yyyy-mm-dd' :: character varying):: text
  ) AS scheduleddate, 
  vst.check_in_datetime AS scheduledtime, 
  vst.status, 
  NULL AS merchandisingresponseid, 
  usr.userdb_id AS salespersonid, 
  NULL AS salesperson_remotekey, 
  usr.first_name AS salesperson_firstname, 
  usr.last_name AS salesperson_lastname, 
  NULL AS salesperson_jobrole, 
  NULL AS salesperson_language, 
  NULL AS salescampaignid, 
  vst_presence.presence, 
  vst_oos.presence AS pricepresence, 
  vst_price.price AS pricedetails, 
  vst_oos.outofstock_cause, 
  NULL :: character varying AS stockcount, 
  'True' AS mustcarryitem, 
  vst_facing.facing AS facings, 
  vst_vert_pos.vertical_pos AS verticalposition, 
  NULL AS storeposition, 
  NULL AS promodetails, 
  vst.sku_id AS productid, 
  prd.barcode AS prod_remotekey, 
  prd.barcode AS eannumber, 
  prd.sku AS productname, 
  prd.country_l1 AS prod_hier_l1, 
  prd.regional_franchise_l2 AS prod_hier_l2, 
  prd.franchise_l3 AS prod_hier_l3, 
  prd.brand_l4 AS prod_hier_l4, 
  prd.sub_category_l5 AS prod_hier_l5, 
  prd.platform_l6 AS prod_hier_l6, 
  prd.variance_l7 AS prod_hier_l7, 
  prd.pack_size_l8 AS prod_hier_l8, 
  prd.sku_english AS prod_hier_l9 
FROM 
  (
    (
      (
        (
          (
            (
              (
                (
                  (
                    SELECT 
                      DISTINCT edw_vw_pop6_visits_sku_audits.visit_id, 
                      edw_vw_pop6_visits_sku_audits.check_in_datetime, 
                      CASE WHEN (
                        edw_vw_pop6_visits_sku_audits.cancelled_visit = 0
                      ) THEN 'completed' :: character varying ELSE 'cancelled' :: character varying END AS status, 
                      edw_vw_pop6_visits_sku_audits.popdb_id, 
                      edw_vw_pop6_visits_sku_audits.sku_id, 
                      edw_vw_pop6_visits_sku_audits.username 
                    FROM 
                      edw_vw_pop6_visits_sku_audits
                  ) vst 
                  LEFT JOIN edw_vw_pop6_store cust ON (
                    (
                      (vst.popdb_id):: text = (cust.popdb_id):: text
                    )
                  )
                ) 
                LEFT JOIN edw_vw_pop6_products prd ON (
                  (
                    (vst.sku_id):: text = (prd.productdb_id):: text
                  )
                )
              ) 
              LEFT JOIN edw_vw_pop6_salesperson usr ON (
                (
                  (vst.username):: text = (usr.username):: text
                )
              )
            ) 
            LEFT JOIN (
              SELECT 
                DISTINCT edw_vw_pop6_visits_sku_audits.visit_id, 
                edw_vw_pop6_visits_sku_audits.popdb_id, 
                edw_vw_pop6_visits_sku_audits.sku_id, 
                edw_vw_pop6_visits_sku_audits.username, 
                CASE WHEN (
                  (
                    upper(
                      (
                        edw_vw_pop6_visits_sku_audits.response
                      ):: text
                    ) = ('YES' :: character varying):: text
                  ) 
                  OR (
                    upper(
                      (
                        edw_vw_pop6_visits_sku_audits.response
                      ):: text
                    ) = ('1' :: character varying):: text
                  )
                ) THEN 'true' :: character varying ELSE 'false' :: character varying END AS presence 
              FROM 
                edw_vw_pop6_visits_sku_audits 
              WHERE 
                (
                  (
                    (
                      edw_vw_pop6_visits_sku_audits.field_code
                    ):: text = ('PS_MSL' :: character varying):: text
                  ) 
                  OR (
                    (
                      (
                        edw_vw_pop6_visits_sku_audits.field_code
                      ):: text = (
                        'Local_Competitor' :: character varying
                      ):: text
                    ) 
                    AND (
                      (
                        edw_vw_pop6_visits_sku_audits.field_label
                      ):: text = (
                        '진열여부' :: character varying
                      ):: text
                    )
                  )
                )
            ) vst_presence ON (
              (
                (
                  (vst.visit_id):: text = (vst_presence.visit_id):: text
                ) 
                AND (
                  (vst.sku_id):: text = (vst_presence.sku_id):: text
                )
              )
            )
          ) 
          LEFT JOIN (
            SELECT 
              DISTINCT oos.visit_id, 
              oos.popdb_id, 
              oos.sku_id, 
              oos.username, 
              CASE WHEN (
                (
                  upper(
                    (oos.response):: text
                  ) = ('YES' :: character varying):: text
                ) 
                OR (
                  upper(
                    (oos.response):: text
                  ) = ('1' :: character varying):: text
                )
              ) THEN 'true' :: character varying ELSE 'false' :: character varying END AS presence, 
              CASE WHEN (
                (
                  upper(
                    (oos.response):: text
                  ) = ('YES' :: character varying):: text
                ) 
                OR (
                  upper(
                    (oos.response):: text
                  ) = ('1' :: character varying):: text
                )
              ) THEN oos_reason.response ELSE NULL :: character varying END AS outofstock_cause 
            FROM 
              (
                edw_vw_pop6_visits_sku_audits oos 
                LEFT JOIN edw_vw_pop6_visits_sku_audits oos_reason ON (
                  (
                    (
                      (oos.visit_id):: text = (oos_reason.visit_id):: text
                    ) 
                    AND (
                      (oos.sku_id):: text = (oos_reason.sku_id):: text
                    )
                  )
                )
              ) 
            WHERE 
              (
                (
                  (oos.field_code):: text = ('PS_MSL_OOS' :: character varying):: text
                ) 
                AND (
                  (oos_reason.field_code):: text = (
                    'PS_MSL_OOS_Reason' :: character varying
                  ):: text
                )
              )
          ) vst_oos ON (
            (
              (
                (vst.visit_id):: text = (vst_oos.visit_id):: text
              ) 
              AND (
                (vst.sku_id):: text = (vst_oos.sku_id):: text
              )
            )
          )
        ) 
        LEFT JOIN (
          SELECT 
            DISTINCT edw_vw_pop6_visits_sku_audits.visit_id, 
            edw_vw_pop6_visits_sku_audits.popdb_id, 
            edw_vw_pop6_visits_sku_audits.sku_id, 
            edw_vw_pop6_visits_sku_audits.username, 
            edw_vw_pop6_visits_sku_audits.response AS price 
          FROM 
            edw_vw_pop6_visits_sku_audits 
          WHERE 
            (
              (
                (
                  (
                    edw_vw_pop6_visits_sku_audits.field_code
                  ):: text = (
                    'Local_MSL_Price' :: character varying
                  ):: text
                ) 
                AND (
                  COALESCE(
                    (
                      (
                        edw_vw_pop6_visits_sku_audits.cntry_cd
                      ):: text || (
                        edw_vw_pop6_visits_sku_audits.response
                      ):: text
                    ), 
                    '' :: text
                  ) <> 'JP定番価格' :: text
                )
              ) 
              AND (
                COALESCE(
                  (
                    (
                      edw_vw_pop6_visits_sku_audits.cntry_cd
                    ):: text || (
                      edw_vw_pop6_visits_sku_audits.response
                    ):: text
                  ), 
                  '' :: text
                ) <> 'JP特売価格' :: text
              )
            )
        ) vst_price ON (
          (
            (
              (vst.visit_id):: text = (vst_price.visit_id):: text
            ) 
            AND (
              (vst.sku_id):: text = (vst_price.sku_id):: text
            )
          )
        )
      ) 
      LEFT JOIN (
        SELECT 
          DISTINCT edw_vw_pop6_visits_sku_audits.visit_id, 
          edw_vw_pop6_visits_sku_audits.popdb_id, 
          edw_vw_pop6_visits_sku_audits.sku_id, 
          edw_vw_pop6_visits_sku_audits.username, 
          edw_vw_pop6_visits_sku_audits.response AS facing 
        FROM 
          edw_vw_pop6_visits_sku_audits 
        WHERE 
          (
            (
              edw_vw_pop6_visits_sku_audits.field_code
            ):: text = (
              'Local_MSL_Facing' :: character varying
            ):: text
          )
      ) vst_facing ON (
        (
          (
            (vst.visit_id):: text = (vst_facing.visit_id):: text
          ) 
          AND (
            (vst.sku_id):: text = (vst_facing.sku_id):: text
          )
        )
      )
    ) 
    LEFT JOIN (
      SELECT 
        DISTINCT edw_vw_pop6_visits_sku_audits.visit_id, 
        edw_vw_pop6_visits_sku_audits.popdb_id, 
        edw_vw_pop6_visits_sku_audits.sku_id, 
        edw_vw_pop6_visits_sku_audits.username, 
        edw_vw_pop6_visits_sku_audits.response AS vertical_pos 
      FROM 
        edw_vw_pop6_visits_sku_audits 
      WHERE 
        (
          (
            edw_vw_pop6_visits_sku_audits.field_code
          ):: text = (
            'Local_MSL_Vertical Position' :: character varying
          ):: text
        )
    ) vst_vert_pos ON (
      (
        (
          (vst.visit_id):: text = (vst_vert_pos.visit_id):: text
        ) 
        AND (
          (vst.sku_id):: text = (vst_vert_pos.sku_id):: text
        )
      )
    )
  )
)
select * from final 