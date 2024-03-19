with itg_th_pos_sales_inventory_fact as(
 select * from {{ ref('thaitg_integration__itg_th_pos_sales_inventory_fact') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),


final as (
SELECT
  derived_table3.sold_to_code,
  derived_table3.sap_prnt_cust_key,
  derived_table3.sap_prnt_cust_desc,
  derived_table3.store_format,
  derived_table3.store,
  derived_table3.trans_date,
  derived_table3.mnth_id,
  derived_table3.barcode,
  derived_table3.matl_num,
  CAST((
    derived_table3.inventory_quantity
  ) AS DOUBLE) AS inventory_quantity,
  derived_table3.list_price,
  derived_table3.inventory_value,
  derived_table3.foc_product
FROM (
  SELECT
    derived_table2.sold_to_code,
    derived_table2.sap_prnt_cust_key,
    derived_table2.sap_prnt_cust_desc,
    derived_table2.store_format,
    derived_table2.store,
    derived_table2.trans_date,
    derived_table2.mnth_id,
    derived_table2.barcode,
    derived_table2.matl_num,
    derived_table2.list_price,
    derived_table2.foc_product,
    SUM(derived_table2.inventory_quantity) AS inventory_quantity,
    SUM(derived_table2.inventory_value) AS inventory_value
  FROM (
    SELECT
      inv_fact.sold_to_code,
      inv_fact.sap_prnt_cust_key,
      inv_fact.sap_prnt_cust_desc,
      CAST(NULL AS VARCHAR) AS store_format,
      inv_fact.branch_code AS store,
      inv_fact.trans_dt AS trans_date,
      time_dim.cal_mnth_id AS mnth_id,
      inv_fact.bar_code AS barcode,
      inv_fact.material_number AS matl_num,
      inv_fact.inventory_qty_converted AS inventory_quantity,
      inv_fact.list_price,
      inv_fact.inventory_gts AS inventory_value,
      inv_fact.foc_product
    FROM (
      (
        itg_th_pos_sales_inventory_fact AS inv_fact
          LEFT JOIN edw_vw_os_time_dim AS time_dim
            ON (
              (
                inv_fact.trans_dt = time_dim.cal_date
              )
            )
      )
      JOIN (
        SELECT
          derived_table1.customer,
          derived_table1.cal_mnth_id,
          MAX(derived_table1.trans_dt) AS max_trans_dt
        FROM (
          SELECT DISTINCT
            itg_th_pos_sales_inventory_fact.customer,
            edw_vw_os_time_dim.cal_mnth_id,
            itg_th_pos_sales_inventory_fact.trans_dt
          FROM (
            itg_th_pos_sales_inventory_fact
              LEFT JOIN edw_vw_os_time_dim
                ON (
                  (
                    edw_vw_os_time_dim.cal_date = itg_th_pos_sales_inventory_fact.trans_dt
                  )
                )
          )
        ) AS derived_table1
        GROUP BY
          derived_table1.customer,
          derived_table1.cal_mnth_id
      ) AS inv_date
        ON (
          (
            (
              (
                CAST((
                  inv_date.customer
                ) AS TEXT) = CAST((
                  inv_fact.customer
                ) AS TEXT)
              )
              AND (
                time_dim.cal_mnth_id = inv_date.cal_mnth_id
              )
            )
            AND (
              inv_date.max_trans_dt = inv_fact.trans_dt
            )
          )
        )
    )
    WHERE
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
  ) AS derived_table2
  GROUP BY
    derived_table2.sold_to_code,
    derived_table2.sap_prnt_cust_key,
    derived_table2.sap_prnt_cust_desc,
    derived_table2.store_format,
    derived_table2.store,
    derived_table2.trans_date,
    derived_table2.mnth_id,
    derived_table2.barcode,
    derived_table2.matl_num,
    derived_table2.list_price,
    derived_table2.foc_product
) AS derived_table3
WHERE
  (
     (
      CONCAT(
        CAST((
          derived_table3.sold_to_code
        ) AS TEXT),
        CAST((
          derived_table3.barcode
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
                    to_date(CAST((
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

