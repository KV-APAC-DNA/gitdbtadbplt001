with itg_th_pos_sales_inventory_fact as(
 select * from {{ ref('thaitg_integration__itg_th_pos_sales_inventory_fact') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_th_pos_customer_dim as (
    select * from {{ ref('thaitg_integration__itg_th_pos_customer_dim') }}
),

final as (
SELECT
  derived_table2.sap_prnt_cust_key,
  derived_table2.sap_prnt_cust_desc,
  derived_table2.sold_to_code,
  derived_table2.store_format,
  derived_table2.store,
  derived_table2.mnth_id,
  derived_table2.barcode,
  derived_table2.matl_num,
  derived_table2.foc_product,
  derived_table2.sellout_quantity,
  derived_table2.sellout_value
FROM (
  SELECT
    derived_table1.sap_prnt_cust_key,
    derived_table1.sap_prnt_cust_desc,
    derived_table1.sold_to_code,
    derived_table1.store_format,
    derived_table1.store,
    derived_table1.mnth_id,
    derived_table1.barcode,
    derived_table1.matl_num,
    derived_table1.foc_product,
    SUM(derived_table1.sellout_quantity) AS sellout_quantity,
    SUM(derived_table1.sellout_value) AS sellout_value
  FROM (
    SELECT
      inv_fact.sap_prnt_cust_key,
      inv_fact.sap_prnt_cust_desc,
      inv_fact.sold_to_code,
      CAST(NULL AS VARCHAR) AS store_format,
      inv_fact.branch_code AS store,
      inv_fact.trans_dt AS trans_date,
      time_dim.cal_mnth_id AS mnth_id,
      inv_fact.bar_code AS barcode,
      inv_fact.material_number AS matl_num,
      inv_fact.foc_product,
      CAST((
        inv_fact.sales_qty_converted
      ) AS DOUBLE) AS sellout_quantity,
      CAST((
        inv_fact.sales_gts
      ) AS DOUBLE) AS sellout_value
    FROM (
      itg_th_pos_sales_inventory_fact AS inv_fact
        LEFT JOIN (
          SELECT DISTINCT
            edw_vw_os_time_dim.cal_date,
            edw_vw_os_time_dim.cal_mnth_id
          FROM edw_vw_os_time_dim
        ) AS time_dim
          ON (
            (
              inv_fact.trans_dt = time_dim.cal_date
            )
          )
    )
    WHERE
      (
        (
          (
            CAST((
              inv_fact.foc_product
            ) AS TEXT) = CAST((
              CAST('N' AS VARCHAR)
            ) AS TEXT)
          )
          OR (
            inv_fact.foc_product IS NULL
          )
        )
        AND (
             (
            CONCAT(
              CAST((
                inv_fact.customer
              ) AS TEXT),
              CAST((
                inv_fact.branch_code
              ) AS TEXT)
            ) NOT IN (
              SELECT DISTINCT
                CONCAT(
                  CAST((
                    itg_th_pos_customer_dim.cust_cd
                  ) AS TEXT),
                  CAST((
                    itg_th_pos_customer_dim.brnch_no
                  ) AS TEXT)
                ) AS CONCAT
              FROM itg_th_pos_customer_dim
              WHERE
                (
                  (
                    UPPER(CAST((
                      itg_th_pos_customer_dim.brnch_typ
                    ) AS TEXT)) = CAST((
                      CAST('DISTRIBUTION CENTER' AS VARCHAR)
                    ) AS TEXT)
                  )
                  AND (
                    CAST((
                      itg_th_pos_customer_dim.cust_cd
                    ) AS TEXT) = CAST((
                      CAST('Lotus' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
            )
          )
        )
      )
  ) AS derived_table1
  GROUP BY
    derived_table1.sap_prnt_cust_key,
    derived_table1.sap_prnt_cust_desc,
    derived_table1.sold_to_code,
    derived_table1.store_format,
    derived_table1.store,
    derived_table1.mnth_id,
    derived_table1.barcode,
    derived_table1.matl_num,
    derived_table1.foc_product
) AS derived_table2
WHERE
  (
     (
      CONCAT(
        CAST((
          derived_table2.sold_to_code
        ) AS TEXT),
        CAST((
          derived_table2.barcode
        ) AS TEXT)
      ) NOT IN (
        SELECT
          CONCAT(
            CAST((
              itg_th_pos_sales_inventory_fact.sold_to_code
            ) AS TEXT),
            CAST((
              itg_th_pos_sales_inventory_fact.bar_code
            ) AS TEXT)
          ) AS CONCAT
        FROM itg_th_pos_sales_inventory_fact
        WHERE
          (
            (
              LEFT(
                CAST((
                  CAST((
                    TO_DATE(CAST((
                      itg_th_pos_sales_inventory_fact.trans_dt
                    ) AS TIMESTAMPNTZ))
                  ) AS VARCHAR)
                ) AS TEXT),
                4
              ) >= CAST((
                CAST((
                  (
                    DATE_PART(
                      YEAR,
                      CAST((
                        CURRENT_TIMESTAMP()
                      ) AS TIMESTAMPNTZ)
                    ) - CAST((
                      2
                    ) AS DOUBLE)
                  )
                ) AS VARCHAR)
              ) AS TEXT)
            )
            AND (
              CAST((
                itg_th_pos_sales_inventory_fact.customer
              ) AS TEXT) = CAST((
                CAST('Lotus' AS VARCHAR)
              ) AS TEXT)
            )
          )
        GROUP BY
          CONCAT(
            CAST((
              itg_th_pos_sales_inventory_fact.sold_to_code
            ) AS TEXT),
            CAST((
              itg_th_pos_sales_inventory_fact.bar_code
            ) AS TEXT)
          )
        HAVING
          (
            CAST((
              SUM(itg_th_pos_sales_inventory_fact.sales_baht)
            ) AS DOUBLE) = CAST((
              0
            ) AS DOUBLE)
          )
      )
    )
  )
)

select * from final
