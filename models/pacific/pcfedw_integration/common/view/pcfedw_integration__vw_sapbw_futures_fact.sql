with edw_time_dim as(
    select *  from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_invoice_fact as(
    select * from {{ ref('aspedw_integration__edw_invoice_fact') }}
), 
dly_sls_cust_attrb_lkp as(
    select * from {{ ref('pcfedw_integration__dly_sls_cust_attrb_lkp') }}
), 
eif as(
SELECT a.act_delv_dt
			,a.act_good_iss_dt
			,a.bill_to_prty
			,a.bill_dt
			,a.bill_typ
			,a.bill_doc
			,a.co_cd
			,a.cust_num
			,a.delv_doc_crt_dt
			,a.dstr_chnl
			,a.div
			,a.doc_crt_dt
			,a.doc_dt
			,a.good_iss_dt
			,a.matl_num
			,a.mat_avail_dt
			,a.ord_rsn
			,a.ovrl_rej_sts
			,a.ovrl_sts_crd_chk
			,a.payer
			,a.plnt
			,a.prec_doc_itm
			,a.prec_doc_num
			,a.proof_delv_dt
			,a.rsn_cd_key
			,a.rsn_rej
			,a.rlse_dt_cr_mgmt
			,a.rqst_delv_dt
			,a.route
			,a.sls_doc
			,a.sls_doc_cat
			,a.sls_doc_itm
			,a.sls_doc_typ
			,a.sls_emp_hist
			,a.sls_org
			,a.sls_doc_itm_cat
			,a.ship_to_prty
			,a.sold_to_prty
			,a.bill_qty_cse
			,a.bill_qty_pc
			,a.bill_qty_difot
			,a.bill_qty_otif
			,a.bill_qty_sls_uom
			,a.cnfrm_qty_difot
			,a.cnfrm_qty_pc
			,a.delv_qty_cse
			,a.delv_qty_pc
			,a.delv_qty_sls_uom
			,a.est_nts
			,a.nts_bill
			,a.net_invc_sls
			,a.fut_sls_qty
			,a.gros_trd_sls
			,a.net_amt
			,a.net_prc
			,a.net_bill_val
			,a.net_ord_val
			,a.ord_qty_cse
			,a.ord_qty_pc
			,a.ord_pc_qty
			,a.ord_sls_qty
			,a.tran_ldtm
			,a.unspp_qty
			,a.unspp_val
			,a.vol_delv
			,a.vol_ord
			,a.cal_day
			,a.base_uom
			,a.curr_key
			,a.doc_curr
			,a.sls_unit
			,a.fisc_yr
			,a.fisc_yr_src
			,a.crt_dttm
			,a.updt_dttm
		FROM edw_invoice_fact a
		WHERE (
				(a.nts_bill <> ((0)::NUMERIC)::NUMERIC(18, 0))
				AND (a.fut_sls_qty <> ((0)::NUMERIC)::NUMERIC(18, 0))
				)
),
a as(
	SELECT eif.co_cd
		,eif.cust_num
		,eif.matl_num
		,sum(eif.fut_sls_qty) AS fut_sls_qty
		,sum(eif.gros_trd_sls) AS gros_trd_sls
		,sum(eif.nts_bill) AS nts_bill
		,eif.sls_doc
		,eif.rqst_delv_dt
		,eif.fisc_yr_src
		,eif.curr_key
	FROM eif
		,(
			SELECT DISTINCT dly_sls_cust_attrb_lkp.cmp_id
			FROM dly_sls_cust_attrb_lkp
			) lkp
	WHERE ((eif.co_cd)::TEXT = (lkp.cmp_id)::TEXT)
	GROUP BY eif.sls_doc
		,eif.rqst_delv_dt
		,eif.cust_num
		,eif.matl_num
		,eif.co_cd
		,eif.fisc_yr_src
		,eif.curr_key
),
transformed as(
SELECT a.co_cd AS cmp_id
	,(
		(
			"substring" ((a.fisc_yr_src)::TEXT,1,4) 
            || "substring" ((a.fisc_yr_src)::TEXT,6,2)
			)
		)::INTEGER AS time_period
	,a.cust_num AS cust_no
	,a.matl_num AS matl_id
	,a.fut_sls_qty AS future_sales_qty
	,a.gros_trd_sls AS future_gts_val
	,0 AS future_trade_val
	,a.nts_bill AS future_nts_val
	,a.curr_key AS local_ccy
FROM  a
	,(
		SELECT DISTINCT edw_time_dim.jj_mnth_id
		FROM edw_time_dim
		WHERE (edw_time_dim.cal_date::TIMESTAMP = DATE_TRUNC('day', current_timestamp::TIMESTAMP))
		) b
WHERE (
		(
					(
						"substring" ((a.fisc_yr_src)::TEXT,1,4)
                        || "substring" ((a.fisc_yr_src)::TEXT,6,2)
						)
					
			)::NUMERIC(18, 0) >= b.jj_mnth_id
		)
)
select * from transformed