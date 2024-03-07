with itg_th_pos_sales_inventory_fact as(
select * from dev_dna_core.snaposeitg_integration.itg_th_pos_sales_inventory_fact
),
itg_th_pos_sales_inventory_fact as(
select * from dev_dna_core.snaposeitg_integration.itg_th_pos_sales_inventory_fact
),
itg_lookup_retention_period as(
select * from dev_dna_core.snaposeitg_integration.itg_lookup_retention_period
),
edw_vw_os_time_dim as(
select * from dev_dna_core.snenav01_workspace.sgpedw_integration__edw_vw_os_time_dim
),
itg_th_pos_customer_dim as(
select * from dev_dna_core.snaposeitg_integration.itg_th_pos_customer_dim
),
edw_vw_os_material_dim as(
select * from dev_dna_core.snaposeedw_integration.edw_vw_os_material_dim
),
so_inv_fact as(
        select
          upper(cast((
            itg_th_pos_sales_inventory_fact.customer
          ) as text)) as customer_code,
          itg_th_pos_sales_inventory_fact.bar_code,
          itg_th_pos_sales_inventory_fact.material_number as item_code,
          itg_th_pos_sales_inventory_fact.trans_dt as invoice_date,
          itg_th_pos_sales_inventory_fact.branch_code,
          itg_th_pos_sales_inventory_fact.list_price,
          itg_th_pos_sales_inventory_fact.retailer_unit_conversion,
          itg_th_pos_sales_inventory_fact.customer_rsp,
          sum(itg_th_pos_sales_inventory_fact.inventory_qty_source) as inventory_qty_source,
          sum(itg_th_pos_sales_inventory_fact.inventory_qty_converted) as inventory_qty_converted,
          sum(itg_th_pos_sales_inventory_fact.inventory_gts) as inventory_gts,
          sum(itg_th_pos_sales_inventory_fact.inventory_baht) as inventory_baht,
          sum(itg_th_pos_sales_inventory_fact.sales_qty_source) as sales_qty_source,
          sum(itg_th_pos_sales_inventory_fact.sales_qty_converted) as sales_qty_converted,
          sum(itg_th_pos_sales_inventory_fact.sales_gts) as sales_gts,
          sum(itg_th_pos_sales_inventory_fact.sales_baht) as sales_baht
        from itg_th_pos_sales_inventory_fact
        where
          (
            coalesce(
              itg_th_pos_sales_inventory_fact.trans_dt,
              current_timestamp()::date
            ) >= (
              select
                DATE_TRUNC('YEAR', DATEADD('DAY', -1 * (itg_lookup_retention_period.retention_years * 365), CURRENT_TIMESTAMP()))::timestamp_ntz AS date_trunc
FROM  itg_lookup_retention_period 
              WHERE
                (
                  upper(cast((
                    itg_lookup_retention_period.table_name
                  ) as text)) = cast((
                    cast('ITG_TH_POS_SALES_FACT' as varchar)
                  ) as text)
                )
            )
          )
        GROUP BY
          UPPER(CAST((
            itg_th_pos_sales_inventory_fact.customer
          ) AS TEXT)),
          itg_th_pos_sales_inventory_fact.bar_code,
          itg_th_pos_sales_inventory_fact.material_number,
          itg_th_pos_sales_inventory_fact.trans_dt,
          itg_th_pos_sales_inventory_fact.branch_code,
          itg_th_pos_sales_inventory_fact.list_price,
          itg_th_pos_sales_inventory_fact.retailer_unit_conversion,
          itg_th_pos_sales_inventory_fact.customer_rsp

),
time_dim as(
SELECT
          edw_vw_os_time_dim."year",
          edw_vw_os_time_dim.qrtr,
          edw_vw_os_time_dim.mnth_id,
          edw_vw_os_time_dim.mnth_no,
          edw_vw_os_time_dim.wk,
          edw_vw_os_time_dim.mnth_wk_no,
          edw_vw_os_time_dim.cal_date
        FROM edw_vw_os_time_dim
),
transformed as(
SELECT
  so_inv_fact.customer_code,
  so_inv_fact.bar_code,
  CAST((
    so_inv_fact.invoice_date
  ) AS TIMESTAMPNTZ) AS invoice_date,
  so_inv_fact.branch_code AS customer_branch_code,
  time_dim."year",
  time_dim.qrtr AS quarter,
  time_dim.mnth_id AS month_year,
  time_dim.mnth_no AS month_number,
  time_dim.wk AS week,
  time_dim.mnth_wk_no AS month_week_number,
  cust_dim.region AS region_name,
  cust_dim.prvnce_cd AS province_code,
  cust_dim.prvnce_eng_nm AS province_name,
  (
    (
      CAST((
        cust_dim.brnch_no
      ) AS TEXT) || CAST((
        CAST(' ' AS VARCHAR)
      ) AS TEXT)
    ) || CAST((
      cust_dim.branch_nm
    ) AS TEXT)
  ) AS branch_name,
  cust_dim.brnch_typ AS branch_type,
  so_inv_fact.item_code,
  matl_dim.sap_mat_desc AS material_description,
  matl_dim.gph_prod_frnchse AS franchise,
  matl_dim.gph_prod_brnd AS brand,
  matl_dim.gph_prod_vrnt AS variant,
  matl_dim.gph_prod_sgmnt AS segment,
  matl_dim.gph_prod_put_up_desc AS put_up,
  so_inv_fact.inventory_qty_converted AS inventory_quantity_piece,
  (
    so_inv_fact.inventory_qty_converted / CAST((
      CAST((
        12
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0))
  ) AS inventory_quantity_dozen,
  so_inv_fact.inventory_baht,
  so_inv_fact.inventory_gts,
  so_inv_fact.sales_qty_converted AS sale_quantity,
  so_inv_fact.sales_baht AS salesbaht,
  so_inv_fact.sales_gts,
  so_inv_fact.retailer_unit_conversion,
  so_inv_fact.list_price,
  so_inv_fact.customer_rsp
FROM (
  (
    ( so_inv_fact LEFT JOIN  time_dim
        ON (
          (
            so_inv_fact.invoice_date = time_dim.cal_date
          )
        )
    )
    LEFT JOIN itg_th_pos_customer_dim AS cust_dim
      ON (
        (
          (
            CAST((
              cust_dim.brnch_no
            ) AS TEXT) = CAST((
              so_inv_fact.branch_code
            ) AS TEXT)
          )
          AND (
            UPPER(so_inv_fact.customer_code) = UPPER(CAST((
              cust_dim.cust_cd
            ) AS TEXT))
          )
        )
      )
  )
  LEFT JOIN edw_vw_os_material_dim AS matl_dim
    ON (
      (
        (
          CAST((
            matl_dim.sap_matl_num
          ) AS TEXT) = CAST((
            so_inv_fact.item_code
          ) AS TEXT)
        )
        AND (
          CAST((
            matl_dim.cntry_key
          ) AS TEXT) = CAST((
            CAST('TH' AS VARCHAR)
          ) AS TEXT)
        )
      )
    )
)
)
select * from transformed