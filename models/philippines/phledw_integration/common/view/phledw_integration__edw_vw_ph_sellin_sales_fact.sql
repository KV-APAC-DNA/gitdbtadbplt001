with edw_copa_trans_fact as (
	select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
t2 as (
	select t1.co_cd,
		t1.ctry_key as cntry_nm,
		t1.caln_day as bill_dt,
		(
			(t1.fisc_yr)::text || "substring" (
				(t1.fisc_yr_per)::text,
				6
				)
			) as jj_mnth_id,
		t1.matl_num as material,
		t1.cust_num as customer,
		t1.sls_org,
		t1.plnt,
		t1.dstr_chnl,
		t1.bill_typ,
		t1.sls_ofc,
		t1.sls_grp,
		t1.sls_dist,
		t1.cust_grp,
		t1.cust_sls,
		t1.fisc_yr,
		t1.pstng_per,
		sum(t1.amt_obj_crncy) as base_val,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Gross Trade Shipment'::TEXT)
					THEN t1.qty
				ELSE (0)::NUMERIC
				END) AS sls_qty,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Returns'::TEXT)
					THEN t1.qty
				ELSE (0)::NUMERIC
				END) AS ret_qty,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Gross Trade Shipment'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS gts_val,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Returns'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS ret_val,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Sales Allowances'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS sls_alwnce,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Cash Discount'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS cash_dscnt,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Hidden Dsct Markdown Allowance'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS hdn_dscnt_mrkdwn_alwnce,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Hidden Dsct Profit Margin'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS hdn_dscnt_prft_mrgn,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Hidden Dsct Promotion'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS hdn_dscnt_prmtn,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Logistics Fees'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS log_fees,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Non-Government Chargebacks'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS ngov_chrgbck,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Pricing/Space Others'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS prc_sp_oth,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Profit Margin Allowance'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS prft_mrgn_alwnce,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Term & Logistic Others'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS trm_log_oth,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Volume Growth Funds'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS vol_grwth_fnds,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Net Trade Sales'::TEXT)
					THEN t1.qty
				ELSE (0)::NUMERIC
				END) AS nts_qty,
		sum(CASE 
				WHEN ((t1.acct_hier_desc)::TEXT = 'Net Trade Sales'::TEXT)
					THEN t1.amt_obj_crncy
				ELSE (0)::NUMERIC
				END) AS nts_val
	from edw_copa_trans_fact t1
	where ((t1.sls_org)::text = '2300'::text) and ctry_key = 'PH'
	group by t1.co_cd,
		t1.ctry_key,
		t1.caln_day,
		t1.fisc_yr_per,
		t1.matl_num,
		t1.cust_num,
		t1.sls_org,
		t1.plnt,
		t1.dstr_chnl,
		t1.bill_typ,
		t1.sls_ofc,
		t1.sls_grp,
		t1.sls_dist,
		t1.cust_grp,
		t1.cust_sls,
		t1.fisc_yr,
		t1.pstng_per
),
transformed as (
	select t2.co_cd,
		t2.cntry_nm,
		t2.bill_dt as pstng_dt,
		(t2.jj_mnth_id)::character varying as jj_mnth_id,
		t2.material as item_cd,
		t2.customer as cust_id,
		t2.sls_org,
		t2.plnt,
		t2.dstr_chnl,
		(null::character varying)::character varying(10) as acct_no,
		t2.bill_typ,
		t2.sls_ofc,
		t2.sls_grp,
		t2.sls_dist,
		t2.cust_grp,
		t2.cust_sls,
		t2.fisc_yr,
		t2.pstng_per,
		t2.base_val,
		t2.sls_qty,
		t2.ret_qty,
		(t2.sls_qty - t2.ret_qty) as sls_less_rtn_qty,
		t2.gts_val,
		t2.ret_val,
		(t2.gts_val - t2.ret_val) as gts_less_rtn_val,
		(null::numeric)::numeric(38, 5) as trdng_term_val,
		(t2.gts_val - (((((((((((t2.sls_alwnce + t2.cash_dscnt) + t2.hdn_dscnt_mrkdwn_alwnce) + t2.hdn_dscnt_prft_mrgn) + t2.hdn_dscnt_prmtn) + t2.log_fees) + t2.ngov_chrgbck) + t2.prc_sp_oth) + t2.prft_mrgn_alwnce) + t2.trm_log_oth) + t2.vol_grwth_fnds) + t2.ret_val)) as tp_val,
		(null::numeric)::numeric(38, 5) as trde_prmtn_val,
		(null::numeric)::numeric(38, 5) as off_inv_trde_prmtn_val,
		t2.nts_val,
		t2.nts_qty,
		(null::numeric)::numeric(38, 5) as cogs
	from t2
)
select * from transformed
