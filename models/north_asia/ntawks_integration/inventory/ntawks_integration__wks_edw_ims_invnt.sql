with itg_ims_invnt as (
select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_IMS_INVNT
),
itg_tw_ims_dstr_prod_map as (
select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_TW_IMS_DSTR_PROD_MAP
),
edw_material_sales_dim as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_MATERIAL_SALES_DIM
),
final as (
SELECT x.invnt_dt,
  x.dstr_cd,
  x.dstr_nm,
  x.prod_cd,
  x.prod_nm,
  CASE 
    WHEN x.ctry_cd = 'TW'
      AND lkp1.ean_cd IS NOT NULL
      THEN lkp1.ean_cd
    WHEN x.ctry_cd = 'HK'
      AND lkp2.ean_cd IS NOT NULL
      THEN lkp2.ean_cd
    ELSE COALESCE(x.ean_num, '#')
    END AS ean_num,
  x.cust_nm,
  x.invnt_qty,
  x.invnt_amt,
  x.avg_prc_amt,
  x.safety_stock,
  x.bad_invnt_qty,
  x.book_invnt_qty,
  x.convs_amt,
  x.prch_disc_amt,
  x.end_invnt_qty,
  x.batch_no,
  x.uom,
  x.sls_rep_cd,
  x.sls_rep_nm,
  x.ctry_cd,
  x.crncy_cd,
  convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) AS crt_dttm,
  convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) AS updt_dttm,
  chn_uom
FROM itg_ims_invnt x
LEFT JOIN itg_tw_ims_dstr_prod_map lkp1 ON x.dstr_cd = lkp1.dstr_cd
  AND (
    x.prod_cd = lkp1.dstr_prod_cd
    OR TRIM(UPPER(x.prod_nm)) = TRIM(UPPER(lkp1.dstr_prod_nm))
    )
LEFT JOIN (
  SELECT MAX(ean_num) AS ean_cd,
    matl_num
  FROM (
    SELECT DISTINCT REPLACE(REPLACE(ean_num, ' ', ''), '-', '') AS ean_num, ---- LTRIM function is removed
      LTRIM(matl_num, 0) AS matl_num
    FROM edw_material_sales_dim
    WHERE ean_num IS NOT NULL
      AND ean_num NOT IN ('', 'NA', 'TBC', 'N/A', 'NIL', 'TBA', '#N/A', 'NOT APPLICABLE', 'NIA', 'N/AA', 'N./A', 'MA', '0')
      AND sls_org IN ('1110', '110S')
      AND dstr_chnl = 10
    )
  GROUP BY matl_num
  ) lkp2 ON LTRIM(trim(x.prod_cd), 0) = lkp2.matl_num
WHERE x.dstr_cd IN ('100496', '100681', '110256', '107479', '107481', '107482', '107483', '107485', '107490', '107507', '107510', '115973', '116047', '122296', '132222', '132349', '120812')

)
select * from final 



