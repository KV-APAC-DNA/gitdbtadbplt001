with edw_my_sellin_prev_dt_snpsht as(
    select * from {{ ref('mysedw_integration__edw_my_sellin_prev_dt_snpsht') }}
),

edw_vw_my_sellin_sales_fact as(
    select * from {{ ref('mysedw_integration__edw_vw_my_sellin_sales_fact') }}
),
transformed as (
    select
  vosst.co_cd,
  vosst.cntry_nm,
  vosst.pstng_dt,
  vosst.jj_mnth_id,
  vosst.item_cd,
  vosst.cust_id,
  vosst.sls_org,
  vosst.plnt,
  vosst.dstr_chnl,
  vosst.acct_no,
  vosst.bill_typ,
  vosst.sls_ofc,
  vosst.sls_grp,
  vosst.sls_dist,
  vosst.cust_grp,
  vosst.cust_sls,
  vosst.fisc_yr,
  vosst.pstng_per,
  (
    vosst.base_val - COALESCE(mspds.base_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS base_val,
  (
    vosst.sls_qty - COALESCE(mspds.sls_qty, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS sls_qty,
  (
    vosst.ret_qty - COALESCE(mspds.ret_qty, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS ret_qty,
  (
    vosst.sls_less_rtn_qty - COALESCE(mspds.sls_less_rtn_qty, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS sls_less_rtn_qty,
  (
    vosst.gts_val - COALESCE(mspds.gts_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS gts_val,
  (
    vosst.ret_val - COALESCE(mspds.ret_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS ret_val,
  (
    vosst.gts_less_rtn_val - COALESCE(mspds.gts_less_rtn_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS gts_less_rtn_val,
  (
    vosst.trdng_term_val - COALESCE(mspds.trdng_term_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS trdng_term_val,
  (
    vosst.tp_val - COALESCE(mspds.tp_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS tp_val,
  (
    vosst.trde_prmtn_val - COALESCE(mspds.trde_prmtn_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS trde_prmtn_val,
  (
    vosst.nts_val - COALESCE(mspds.nts_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS nts_val,
  (
    vosst.nts_qty - COALESCE(mspds.nts_qty, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS nts_qty,
  'Y' AS is_curr
FROM (
  edw_vw_my_sellin_sales_fact AS vosst
    LEFT JOIN edw_my_sellin_prev_dt_snpsht AS mspds
      ON 
      (coalesce(mspds.co_cd::TEXT,'') = (coalesce(vosst.co_cd::TEXT,'')))
			AND (coalesce(mspds.cntry_nm::TEXT,'') = (coalesce(vosst.cntry_nm::TEXT,'')))
			AND (coalesce(mspds.pstng_dt::TEXT,'') = (coalesce(vosst.pstng_dt::TEXT,'')))
			AND (coalesce(mspds.jj_mnth_id::TEXT,'') = (coalesce(vosst.jj_mnth_id::TEXT,'')))
			AND (coalesce(mspds.item_cd::TEXT,'') = (coalesce(vosst.item_cd::TEXT,'')))
			AND (coalesce(mspds.cust_id::TEXT,'') = (coalesce(vosst.cust_id::TEXT,'')))
			AND (coalesce(mspds.sls_org::TEXT,'') = (coalesce(vosst.sls_org::TEXT,'')))
			AND (coalesce(mspds.plnt::TEXT,'') = (coalesce(vosst.plnt::TEXT,'')))
			AND (coalesce(mspds.dstr_chnl::TEXT,'') = (coalesce(vosst.dstr_chnl::TEXT,'')))
			AND (coalesce(mspds.acct_no::TEXT,'') = (coalesce(vosst.acct_no::TEXT,'')))
			AND (coalesce(mspds.bill_typ::TEXT,'') = (coalesce(vosst.bill_typ::TEXT,'')))
			AND (coalesce(mspds.sls_ofc::TEXT,'') = (coalesce(vosst.sls_ofc::TEXT,'')))
			AND (coalesce(mspds.sls_grp::TEXT,'') = (coalesce(vosst.sls_grp::TEXT,'')))
			AND (coalesce(mspds.sls_dist::TEXT,'') = (coalesce(vosst.sls_dist::TEXT,'')))
			AND (coalesce(mspds.cust_grp::TEXT,'') = (coalesce(vosst.cust_grp::TEXT,'')))
			AND (coalesce(mspds.cust_sls::TEXT,'') = (coalesce(vosst.cust_sls::TEXT,'')))
			AND (coalesce(mspds.fisc_yr::text,'') = coalesce(vosst.fisc_yr::text,''))
			AND (coalesce(mspds.pstng_per::text,'') = coalesce(vosst.pstng_per::text,''))
)
WHERE
(
		(
			((vosst.base_val - COALESCE(mspds.base_val, ((0)::NUMERIC)::NUMERIC(18, 0))) <> ((0)::NUMERIC)::NUMERIC(18, 0))
			OR ((vosst.sls_qty - COALESCE(mspds.sls_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) <> ((0)::NUMERIC)::NUMERIC(18, 0))
			OR ((vosst.ret_qty - COALESCE(mspds.ret_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) <> ((0)::NUMERIC)::NUMERIC(18, 0))
			OR ((vosst.sls_less_rtn_qty - COALESCE(mspds.sls_less_rtn_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) <> ((0)::NUMERIC)::NUMERIC(18, 0))
			OR ((vosst.gts_val - COALESCE(mspds.gts_val, ((0)::NUMERIC)::NUMERIC(18, 0))) <> ((0)::NUMERIC)::NUMERIC(18, 0))
			OR ((vosst.ret_val - COALESCE(mspds.ret_val, ((0)::NUMERIC)::NUMERIC(18, 0))) <> ((0)::NUMERIC)::NUMERIC(18, 0))
			OR ((vosst.gts_less_rtn_val - COALESCE(mspds.gts_less_rtn_val, ((0)::NUMERIC)::NUMERIC(18, 0))) <> ((0)::NUMERIC)::NUMERIC(18, 0))
			OR ((vosst.trdng_term_val - COALESCE(mspds.trdng_term_val, ((0)::NUMERIC)::NUMERIC(18, 0))) <> ((0)::NUMERIC)::NUMERIC(18, 0))
			OR ((vosst.tp_val - COALESCE(mspds.tp_val, ((0)::NUMERIC)::NUMERIC(18, 0))) <> ((0)::NUMERIC)::NUMERIC(18, 0))
			OR ((vosst.trde_prmtn_val - COALESCE(mspds.trde_prmtn_val, ((0)::NUMERIC)::NUMERIC(18, 0))) <> ((0)::NUMERIC)::NUMERIC(18, 0))
			OR ((vosst.nts_val - COALESCE(mspds.nts_val, ((0)::NUMERIC)::NUMERIC(18, 0))) <> ((0)::NUMERIC)::NUMERIC(18, 0))
			OR ((vosst.nts_qty - COALESCE(mspds.nts_qty, ((0)::NUMERIC)::NUMERIC(18, 0))) <> ((0)::NUMERIC)::NUMERIC(18, 0))
			)
		AND ((vosst.cntry_nm)::TEXT = ('MY'::CHARACTER VARYING)::TEXT)
		)
UNION ALL
select
  edw_my_sellin_prev_dt_snpsht.co_cd,
  edw_my_sellin_prev_dt_snpsht.cntry_nm,
  edw_my_sellin_prev_dt_snpsht.pstng_dt,
  edw_my_sellin_prev_dt_snpsht.jj_mnth_id,
  edw_my_sellin_prev_dt_snpsht.item_cd,
  edw_my_sellin_prev_dt_snpsht.cust_id,
  edw_my_sellin_prev_dt_snpsht.sls_org,
  edw_my_sellin_prev_dt_snpsht.plnt,
  edw_my_sellin_prev_dt_snpsht.dstr_chnl,
  edw_my_sellin_prev_dt_snpsht.acct_no,
  edw_my_sellin_prev_dt_snpsht.bill_typ,
  edw_my_sellin_prev_dt_snpsht.sls_ofc,
  edw_my_sellin_prev_dt_snpsht.sls_grp,
  edw_my_sellin_prev_dt_snpsht.sls_dist,
  edw_my_sellin_prev_dt_snpsht.cust_grp,
  edw_my_sellin_prev_dt_snpsht.cust_sls,
  edw_my_sellin_prev_dt_snpsht.fisc_yr,
  edw_my_sellin_prev_dt_snpsht.pstng_per,
  edw_my_sellin_prev_dt_snpsht.base_val,
  edw_my_sellin_prev_dt_snpsht.sls_qty,
  edw_my_sellin_prev_dt_snpsht.ret_qty,
  edw_my_sellin_prev_dt_snpsht.sls_less_rtn_qty,
  edw_my_sellin_prev_dt_snpsht.gts_val,
  edw_my_sellin_prev_dt_snpsht.ret_val,
  edw_my_sellin_prev_dt_snpsht.gts_less_rtn_val,
  edw_my_sellin_prev_dt_snpsht.trdng_term_val,
  edw_my_sellin_prev_dt_snpsht.tp_val,
  edw_my_sellin_prev_dt_snpsht.trde_prmtn_val,
  edw_my_sellin_prev_dt_snpsht.nts_val,
  edw_my_sellin_prev_dt_snpsht.nts_qty,
  'N' AS is_curr
from edw_my_sellin_prev_dt_snpsht
),

final as(
    select 
    co_cd as co_cd,
    cntry_nm as cntry_nm,
    pstng_dt as pstng_dt,
    jj_mnth_id as jj_mnth_id,
    item_cd as item_cd,
    cust_id as cust_id,
    sls_org as sls_org,
    plnt as plnt,
    dstr_chnl as dstr_chnl,
    acct_no as acct_no,
    bill_typ as bill_typ,
    sls_ofc as sls_ofc,
    sls_grp as sls_grp,
    sls_dist as sls_dist,
    cust_grp as cust_grp,
    cust_sls as cust_sls,
    fisc_yr as fisc_yr,
    pstng_per as pstng_per,
    base_val as base_val,
    sls_qty as sls_qty,
    ret_qty as ret_qty,
    sls_less_rtn_qty as sls_less_rtn_qty,
    gts_val as gts_val,
    ret_val as ret_val,
    gts_less_rtn_val as gts_less_rtn_val,
    trdng_term_val as trdng_term_val,
    tp_val as tp_val,
    trde_prmtn_val as trde_prmtn_val,
    nts_val as nts_val,
    nts_qty as nts_qty,
    is_curr as is_curr 
    from transformed
)

select * from final