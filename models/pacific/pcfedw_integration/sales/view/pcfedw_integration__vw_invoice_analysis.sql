with vw_bwar_curr_exch_dim as(
	select * from {{ ref('pcfedw_integration__vw_bwar_curr_exch_dim') }}
),
vw_jjbr_curr_exch_dim as(
	select * from {{ ref('pcfedw_integration__vw_jjbr_curr_exch_dim') }}
),
edw_invoice_fact as(
	select * from {{ ref('aspedw_integration__edw_invoice_fact') }}
),
vw_customer_dim as(
	select * from {{ ref('pcfedw_integration__vw_customer_dim') }}
),
edw_customer_base_dim as(
	select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_time_dim as(
	select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
vw_material_dim as(
	select * from {{ ref('pcfedw_integration__vw_material_dim') }}
),
currex as(
		SELECT vw_jjbr_curr_exch_dim.rate_type
		,vw_jjbr_curr_exch_dim.from_ccy
		,vw_jjbr_curr_exch_dim.to_ccy
		,vw_jjbr_curr_exch_dim.jj_mnth_id
		,vw_jjbr_curr_exch_dim.exch_rate
	FROM vw_jjbr_curr_exch_dim
	WHERE (
			(vw_jjbr_curr_exch_dim.exch_rate = (1)::NUMERIC(15, 5))
			AND ((vw_jjbr_curr_exch_dim.from_ccy)::TEXT = 'AUD'::TEXT)
			)
	
	UNION ALL
	
	SELECT vw_bwar_curr_exch_dim.rate_type
		,vw_bwar_curr_exch_dim.from_ccy
		,vw_bwar_curr_exch_dim.to_ccy
		,vw_bwar_curr_exch_dim.jj_mnth_id
		,vw_bwar_curr_exch_dim.exch_rate
	FROM vw_bwar_curr_exch_dim
),
transformed as(
SELECT ltrim((vmd.matl_id)::TEXT, (0)::TEXT) AS matl_id
	,vmd.matl_desc
	,vmd.mega_brnd_cd
	,vmd.mega_brnd_desc
	,vmd.brnd_cd
	,vmd.brnd_desc
	,vmd.base_prod_cd
	,vmd.base_prod_desc
	,vmd.variant_cd
	,vmd.variant_desc
	,vmd.fran_cd
	,vmd.fran_desc
	,vmd.grp_fran_cd
	,vmd.grp_fran_desc
	,vmd.matl_type_cd
	,vmd.matl_type_desc
	,vmd.prod_fran_cd
	,vmd.prod_fran_desc
	,vmd.prod_hier_cd
	,vmd.prod_hier_desc
	,vmd.prod_mjr_cd
	,vmd.prod_mjr_desc
	,vmd.prod_mnr_cd
	,vmd.prod_mnr_desc
	,vmd.mercia_plan
	,vmd.putup_cd
	,vmd.putup_desc
	,vmd.bar_cd
	,vmd.updt_dt
	,vcd.cust_no
	,vcd.cmp_id
	,vcd.channel_cd
	,vcd.channel_desc
	,vcd.ctry_key
	,vcd.country
	,vcd.state_cd
	,vcd.post_cd
	,vcd.cust_suburb
	,vcd.cust_nm
	,vcd.sls_org
	,vcd.cust_del_flag
	,vcd.sales_office_cd
	,vcd.sales_office_desc
	,vcd.sales_grp_cd
	,vcd.sales_grp_desc
	,vcd.mercia_ref
	,vcd.curr_cd
	,etm.cal_date
	,etm.time_id
	,etm.jj_wk
	,etm.jj_mnth
	,etm.jj_mnth_shrt
	,etm.jj_mnth_long
	,etm.jj_qrtr
	,etm.jj_year
	,etm.cal_mnth_id
	,etm.jj_mnth_id
	,etm.cal_mnth
	,etm.cal_qrtr
	,etm.cal_year
	,etm.jj_mnth_tot
	,etm.jj_mnth_day
	,etm.cal_mnth_nm
	,eif.act_delv_dt
	,eif.act_good_iss_dt
	,eif.base_uom
	,eif.bill_doc
	,eif.bill_dt
	,eif.bill_qty_cse
	,eif.bill_qty_difot
	,eif.bill_qty_otif
	,eif.bill_qty_pc
	,eif.bill_qty_sls_uom
	,eif.bill_to_prty
	,eif.bill_typ
	,eif.cal_day
	,eif.cnfrm_qty_difot
	,eif.cnfrm_qty_pc
	,eif.co_cd
	,eif.crt_dttm
	,eif.curr_key
	,eif.cust_num
	,eif.delv_doc_crt_dt
	,eif.delv_qty_cse
	,eif.delv_qty_pc
	,eif.delv_qty_sls_uom
	,eif.div
	,eif.doc_crt_dt
	,eif.doc_curr
	,eif.doc_dt
	,eif.dstr_chnl
	,eif.est_nts
	,eif.fisc_yr
	,eif.fisc_yr_src
	,eif.fut_sls_qty
	,eif.good_iss_dt
	,(ZEROIFNULL((eif.gros_trd_sls/NULLIF(eif.fut_sls_qty,0))*eif.cnfrm_qty_pc) * currex.exch_rate) AS future_gts_val
	,eif.mat_avail_dt
	,ltrim((eif.matl_num)::TEXT, (0)::TEXT) AS matl_num
	,eif.net_amt
	,eif.net_bill_val
	,eif.net_invc_sls
	,eif.net_ord_val
	,eif.net_prc
	,(eif.nts_bill * currex.exch_rate) AS nts_val
	,eif.ord_pc_qty
	,eif.ord_qty_cse
	,eif.ord_qty_pc
	,eif.ord_rsn
	,eif.ord_sls_qty
	,eif.ovrl_rej_sts
	,eif.ovrl_sts_crd_chk
	,eif.payer
	,eif.plnt
	,eif.prec_doc_itm
	,eif.prec_doc_num
	,eif.proof_delv_dt
	,eif.rlse_dt_cr_mgmt
	,eif.route
	,eif.rqst_delv_dt
	,eif.rsn_cd_key
	,eif.rsn_rej
	,eif.ship_to_prty
	,cbd.cust_nm AS ship_name
	,eif.sls_doc
	,eif.sls_doc_cat
	,eif.sls_doc_itm
	,eif.sls_doc_itm_cat
	,eif.sls_doc_typ
	,eif.sls_emp_hist
	,eif.sls_unit
	,eif.sold_to_prty
	,eif.tran_ldtm
	,eif.unspp_qty
	,eif.unspp_val
	,eif.updt_dttm
	,eif.vol_delv
	,eif.vol_ord
	,currex.to_ccy
	,currex.exch_rate
FROM currex
	,(
		(
			(
				(
					edw_invoice_fact eif LEFT JOIN vw_customer_dim vcd ON (((eif.cust_num)::TEXT = (vcd.cust_no)::TEXT))
					) LEFT JOIN vw_material_dim vmd ON (((eif.matl_num)::TEXT = (vmd.matl_id)::TEXT))
				) LEFT JOIN edw_time_dim etm ON (((eif.rqst_delv_dt)::TIMESTAMP = etm.cal_date))
			) LEFT JOIN edw_customer_base_dim cbd ON (((eif.ship_to_prty)::TEXT = (cbd.cust_num)::TEXT))
		)
WHERE (
		(
			((eif.curr_key)::TEXT = (currex.from_ccy)::TEXT)
			AND (etm.jj_mnth_id = currex.jj_mnth_id)
			)
		AND ((etm.jj_year)::DOUBLE PRECISION >= (EXTRACT(YEAR FROM CURRENT_TIMESTAMP()) - 1::DOUBLE PRECISION))
		)
)

select * from transformed