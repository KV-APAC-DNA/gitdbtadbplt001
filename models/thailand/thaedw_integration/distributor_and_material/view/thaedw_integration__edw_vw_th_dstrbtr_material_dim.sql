with itg_th_dstrbtr_material_dim as (
select * from {{ ref('thaitg_integration__itg_th_dstrbtr_material_dim') }}
),
edw_material_sales_dim as (
select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
final as (

    select 
  'TH' AS cntry_cd, 
  'Thailand' AS cntry_nm, 
  NULL AS dstrbtr_grp_cd, 
  NULL AS dstrbtr_soldto_code, 
  NULL AS sap_soldto_code, 
  (
    ltrim(
      (m.item_cd):: TEXT, 
      (0):: TEXT
    )
  ) AS dstrbtr_matl_num, 
  m.name_eng AS dstrbtr_matl_desc, 
  NULL AS dstrbtr_alt_matl_num, 
  NULL AS dstrbtr_alt_matl_desc, 
  NULL AS dstrbtr_alt2_matl_num, 
  NULL AS dstrbtr_alt2_matl_desc, 
  (
    ltrim(
      (m.bar_cd):: TEXT, 
      (0):: TEXT
    )
  ) AS dstrbtr_bar_cd, 
  (
    ltrim(
      (m.item_cd):: TEXT, 
      (0):: TEXT
    )
  ) AS sap_matl_num, 
  (m.unit_per_case) AS pc_per_case, 
  m.is_npi, 
  (m.npi_start_dt) AS npi_str_period, 
  (m.npi_end_dt) AS npi_end_period, 
  (
    CASE WHEN (sd.reg_matl_num IS NULL) THEN 'N' :: TEXT ELSE 'Y' :: TEXT END
  ) AS is_reg, 
  NULL AS is_promo, 
  NULL AS promo_strt_period, 
  NULL AS promo_end_period, 
  NULL AS is_mcl, 
  m.is_hero, 
  NULL AS eff_strt_dt, 
  NULL AS eff_end_dt 
FROM 
  (
    itg_th_dstrbtr_material_dim m 
    LEFT JOIN (
      SELECT 
        DISTINCT ltrim(
          (
            edw_material_sales_dim.matl_num
          ):: TEXT, 
          (0):: TEXT
        ) AS reg_matl_num 
      FROM 
        edw_material_sales_dim 
      WHERE 
        (
          (
            (
              (edw_material_sales_dim.sls_org):: TEXT = '2400' :: TEXT
            ) 
            AND (
              ltrim(
                (
                  edw_material_sales_dim.prod_type_apo
                ):: TEXT, 
                (0):: TEXT
              ) = '1' :: TEXT
            )
          ) 
          AND (
            CASE WHEN (
              (
                edw_material_sales_dim.matl_num
              ):: TEXT = '' :: TEXT
            ) THEN NULL ELSE edw_material_sales_dim.matl_num END IS NOT NULL
          )
        )
    ) sd ON (
      (
        ltrim(
          (m.item_cd):: TEXT, 
          (0):: TEXT
        ) = sd.reg_matl_num
      )
    )
  )
)

select * from final