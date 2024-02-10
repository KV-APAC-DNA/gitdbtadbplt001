with edw_vw_my_sipos_analysis as 
( 
   select * from {{ ref('mysedw_integration__edw_vw_my_sipos_analysis') }}
),
edw_vw_my_material_dim as ( 
  select * from {{ ref('mysedw_integration__edw_vw_my_material_dim') }}
),
edw_vw_my_listprice as (
  select * from {{ ref('mysedw_integration__edw_vw_my_listprice') }}
),
edw_vw_os_time_dim as (
  select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_my_pos_cust_mstr as (
  select * from {{ ref('mysitg_integration__itg_my_pos_cust_mstr') }}
),
itg_my_customer_dim as (
  select * from {{ ref('mysitg_integration__itg_my_customer_dim') }}
),
td as (
      SELECT DISTINCT
        edw_vw_os_time_dim.cal_year AS year,
        edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
        edw_vw_os_time_dim.cal_mnth_id AS mnth_id
      FROM edw_vw_os_time_dim
    ),
union_1 as
(SELECT
  sipos.jj_year AS year,
  td.qrtr_no,
  CAST((
    sipos.jj_mnth_id
  ) AS INT) AS mnth_id,
  sipos.jj_mnth_no AS mnth_no,
  sipos.cntry_nm,
  'SIPOS' AS data_src,
  sipos.dstrbtr_grp_cd,
  sipos.global_prod_franchise,
  sipos.global_prod_brand,
  sipos.global_prod_sub_brand,
  sipos.global_prod_variant,
  sipos.global_prod_segment,
  sipos.global_prod_subsegment,
  sipos.global_prod_category,
  sipos.global_prod_subcategory,
  sipos.global_put_up_desc,
  sipos.sku,
  sipos.sku_desc,
  sipos.bill_qty_pc,
  sipos.billing_grs_trd_sls,
  0.0 AS end_stock_qty,
  0.0 AS end_stock_val,
  sipos.jj_pos_qty_pc AS sls_qty_pc,
  sipos.jj_pos_gts AS jj_grs_trd_sls
FROM (
  edw_vw_my_sipos_analysis AS sipos
    LEFT JOIN  td
      ON (
        (CAST((  CAST(( td.mnth_id ) AS VARCHAR)) AS TEXT) = CAST((  sipos.jj_mnth_id) AS TEXT)
        )
      )
)
WHERE
  (
    (
      CAST((  sipos.to_ccy) AS TEXT) = CAST((  CAST('MYR' AS VARCHAR)) AS TEXT)
    )
    AND 
    (
      COALESCE(sipos.sold_to, CAST('NA' AS VARCHAR)) IN 
      (
        SELECT DISTINCT
          COALESCE(b.cust_id, CAST('NA' AS VARCHAR)) 
        FROM itg_my_pos_cust_mstr AS a, itg_my_customer_dim AS b
        WHERE
          (
            (CAST((  b.dstrbtr_grp_cd) AS TEXT) = CAST((  a.cust_id) AS TEXT)
            )
            AND upper((a.cust_nm)::text) LIKE ('%WATSON%'::varchar)::text
            
          )
      )
    )
  )
),
veomd as
(
  SELECT
    edw_vw_my_material_dim.cntry_key,
    edw_vw_my_material_dim.sap_matl_num,
    edw_vw_my_material_dim.sap_mat_desc,
    edw_vw_my_material_dim.ean_num,
    edw_vw_my_material_dim.sap_mat_type_cd,
    edw_vw_my_material_dim.sap_mat_type_desc,
    edw_vw_my_material_dim.sap_base_uom_cd,
    edw_vw_my_material_dim.sap_prchse_uom_cd,
    edw_vw_my_material_dim.sap_prod_sgmt_cd,
    edw_vw_my_material_dim.sap_prod_sgmt_desc,
    edw_vw_my_material_dim.sap_base_prod_cd,
    edw_vw_my_material_dim.sap_base_prod_desc,
    edw_vw_my_material_dim.sap_mega_brnd_cd,
    edw_vw_my_material_dim.sap_mega_brnd_desc,
    edw_vw_my_material_dim.sap_brnd_cd,
    edw_vw_my_material_dim.sap_brnd_desc,
    edw_vw_my_material_dim.sap_vrnt_cd,
    edw_vw_my_material_dim.sap_vrnt_desc,
    edw_vw_my_material_dim.sap_put_up_cd,
    edw_vw_my_material_dim.sap_put_up_desc,
    edw_vw_my_material_dim.sap_grp_frnchse_cd,
    edw_vw_my_material_dim.sap_grp_frnchse_desc,
    edw_vw_my_material_dim.sap_frnchse_cd,
    edw_vw_my_material_dim.sap_frnchse_desc,
    edw_vw_my_material_dim.sap_prod_frnchse_cd,
    edw_vw_my_material_dim.sap_prod_frnchse_desc,
    edw_vw_my_material_dim.sap_prod_mjr_cd,
    edw_vw_my_material_dim.sap_prod_mjr_desc,
    edw_vw_my_material_dim.sap_prod_mnr_cd,
    edw_vw_my_material_dim.sap_prod_mnr_desc,
    edw_vw_my_material_dim.sap_prod_hier_cd,
    edw_vw_my_material_dim.sap_prod_hier_desc,
    edw_vw_my_material_dim.gph_region,
    edw_vw_my_material_dim.gph_reg_frnchse,
    edw_vw_my_material_dim.gph_reg_frnchse_grp,
    edw_vw_my_material_dim.gph_prod_frnchse,
    edw_vw_my_material_dim.gph_prod_brnd,
    edw_vw_my_material_dim.gph_prod_sub_brnd,
    edw_vw_my_material_dim.gph_prod_vrnt,
    edw_vw_my_material_dim.gph_prod_needstate,
    edw_vw_my_material_dim.gph_prod_ctgry,
    edw_vw_my_material_dim.gph_prod_subctgry,
    edw_vw_my_material_dim.gph_prod_sgmnt,
    edw_vw_my_material_dim.gph_prod_subsgmnt,
    edw_vw_my_material_dim.gph_prod_put_up_cd,
    edw_vw_my_material_dim.gph_prod_put_up_desc,
    edw_vw_my_material_dim.gph_prod_size,
    edw_vw_my_material_dim.gph_prod_size_uom,
    edw_vw_my_material_dim.launch_dt,
    edw_vw_my_material_dim.qty_shipper_pc,
    edw_vw_my_material_dim.prft_ctr,
    edw_vw_my_material_dim.shlf_life
  FROM edw_vw_my_material_dim
) ,
lp_jj as
(
  SELECT
    edw_vw_my_listprice.cntry_key,
    edw_vw_my_listprice.cntry_nm,
    edw_vw_my_listprice.plant,
    edw_vw_my_listprice.cnty,
    edw_vw_my_listprice.item_cd,
    edw_vw_my_listprice.item_desc,
    edw_vw_my_listprice.valid_from,
    edw_vw_my_listprice.valid_to,
    edw_vw_my_listprice.rate,
    edw_vw_my_listprice.currency,
    edw_vw_my_listprice.price_unit,
    edw_vw_my_listprice.uom,
    edw_vw_my_listprice.yearmo,
    edw_vw_my_listprice.mnth_type,
    edw_vw_my_listprice.snapshot_dt
  FROM edw_vw_my_listprice
  WHERE
    (
       (CAST((  edw_vw_my_listprice.mnth_type) AS TEXT) = CAST((  CAST('JJ' AS VARCHAR)) AS TEXT)
      )
    )
) ,
lp_cal as
(
  SELECT
    edw_vw_my_listprice.cntry_key,
    edw_vw_my_listprice.cntry_nm,
    edw_vw_my_listprice.plant,
    edw_vw_my_listprice.cnty,
    edw_vw_my_listprice.item_cd,
    edw_vw_my_listprice.item_desc,
    edw_vw_my_listprice.valid_from,
    edw_vw_my_listprice.valid_to,
    edw_vw_my_listprice.rate,
    edw_vw_my_listprice.currency,
    edw_vw_my_listprice.price_unit,
    edw_vw_my_listprice.uom,
    edw_vw_my_listprice.yearmo,
    edw_vw_my_listprice.mnth_type,
    edw_vw_my_listprice.snapshot_dt
  FROM edw_vw_my_listprice
  WHERE
    (
    (CAST((  edw_vw_my_listprice.mnth_type) AS TEXT) = CAST((  CAST('CAL' AS VARCHAR)) AS TEXT)
      )
    )
),
veotd as (
  SELECT DISTINCT
    edw_vw_os_time_dim.cal_year AS year,
    edw_vw_os_time_dim.cal_qrtr_no AS qrtr_no,
    edw_vw_os_time_dim.cal_mnth_id AS mnth_id,
    edw_vw_os_time_dim.cal_mnth_no AS mnth_no,
    edw_vw_os_time_dim.cal_mnth_nm AS mnth_nm
  FROM edw_vw_os_time_dim
  GROUP BY
    edw_vw_os_time_dim.cal_year,
    edw_vw_os_time_dim.cal_qrtr_no,
    edw_vw_os_time_dim.cal_mnth_id,
    edw_vw_os_time_dim.cal_mnth_no,
    edw_vw_os_time_dim.cal_mnth_nm
),
union_2 as 
(
  SELECT
  CAST((inv.year) AS INT) AS year,
  veotd.qrtr_no,
  CAST(( inv.mnth_id) AS INT) AS mnth_id,
  veotd.mnth_no,
  'Malaysia' AS cntry_nm,
  'INVPOS' AS data_src,
  inv.cust_cd AS dstrbtr_grp_cd,
  veomd.gph_prod_frnchse AS global_prod_franchise,
  veomd.gph_prod_brnd AS global_prod_brand,
  veomd.gph_prod_sub_brnd AS global_prod_sub_brand,
  veomd.gph_prod_vrnt AS global_prod_variant,
  veomd.gph_prod_sgmnt AS global_prod_segment,
  veomd.gph_prod_subsgmnt AS global_prod_subsegment,
  veomd.gph_prod_ctgry AS global_prod_category,
  veomd.gph_prod_subctgry AS global_prod_subcategory,
  veomd.gph_prod_put_up_desc AS global_put_up_desc,
  CAST((LTRIM(CAST((  inv.sap_matl_num) AS TEXT), CAST((  CAST('0' AS VARCHAR)) AS TEXT))) AS VARCHAR) AS sku,
  veomd.sap_mat_desc AS sku_desc,
  0.0 AS bill_qty_pc,
  0.0 AS billing_grs_trd_sls,
  inv.end_stock_qty,
  (inv.end_stock_qty * COALESCE(lp_cal.rate, lp_jj.rate)) AS end_stock_val,
  0.0 AS sls_qty_pc,
  0.0 AS jj_grs_trd_sls
FROM  veotd, 
(
  (
    (
      edw_vw_my_pos_inventory AS inv
        LEFT JOIN  lp_cal
          ON (
              (LTRIM(CAST((  lp_cal.item_cd) AS TEXT), CAST((  CAST('0' AS VARCHAR)) AS TEXT)) 
              = LTRIM(CAST((  inv.sap_matl_num) AS TEXT), CAST((  CAST('0' AS VARCHAR)) AS TEXT))
              )
              AND
              (
                CAST((  lp_cal.yearmo) AS TEXT) = CAST((  inv.mnth_id) AS TEXT)
              )

          )
    )
    LEFT JOIN lp_jj
      ON (
        (
          (LTRIM(CAST((  lp_jj.item_cd) AS TEXT), CAST((  CAST('0' AS VARCHAR)) AS TEXT)) 
          = LTRIM(CAST((  inv.sap_matl_num) AS TEXT), CAST((  CAST('0' AS VARCHAR)) AS TEXT))
          )
          AND (CAST((  lp_jj.yearmo) AS TEXT) = CAST((  inv.mnth_id) AS TEXT)
          )
        )
      )
  )
  LEFT JOIN veomd
    ON (
      (LTRIM(CAST((  inv.sap_matl_num) AS TEXT), CAST((  CAST('0' AS VARCHAR)) AS TEXT)) 
      = LTRIM(CAST((  veomd.sap_matl_num) AS TEXT), CAST((  CAST('0' AS VARCHAR)) AS TEXT))
      )
    )
)
WHERE
  (CAST((  CAST((veotd.mnth_id  ) AS VARCHAR)) AS TEXT) = CAST((inv.mnth_id) AS TEXT)
  )
),
final as 
( 
  select * from union_1 
  union all 
    select * from union_2
)
select * from final