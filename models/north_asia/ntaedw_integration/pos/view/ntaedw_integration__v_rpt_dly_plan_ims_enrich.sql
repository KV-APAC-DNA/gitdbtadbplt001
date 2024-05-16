with
edw_store_dim as 
(
    select * from snapntaedw_integration.edw_store_dim
),
v_dly_planned_ims as 
(
    select * from snapntaedw_integration.v_dly_planned_ims
),
v_dly_ims_txn_msl as 
(
    select * from snapntaedw_integration.v_dly_ims_txn_msl
),
v_intrm_calendar_ims as 
(
    select * from snapntaedw_integration.v_intrm_calendar_ims
),
edw_product_attr_dim as 
(
    select * from snapaspedw_integration.edw_product_attr_dim
), --rg schema
edw_sls_rep_dim as 
(
    select * from snapntaedw_integration.edw_sls_rep_dim
),
edw_sls_rep_dim as 
(
    select * from snapntaedw_integration.edw_sls_rep_dim
), --source from nta
v_intrm_crncy_exch as 
(
    select * from snapntaedw_integration.v_intrm_crncy_exch
),
final as
(
    SELECT COALESCE(pln.cal_day, txn.ims_txn_dt) AS ims_txn_dt
	,b.fisc_per
	,b.cal_wk AS fisc_wk
	,b.no_of_wks
	,b.fisc_wk_num
	,pln.visit_dt
	,pln.visit_jj_mnth_id
	,pln.visit_jj_wk_no
	,pln.visit_jj_wkdy_no
	,pln.visit_end_dt
	,txn.ims_txn_dt AS actl_ims_txn_dt
	,COALESCE(pln.ctry_cd, txn.ctry_cd) AS ctry_cd
	,COALESCE(pln.dstr_cd, txn.dstr_cd) AS dstr_cd
	,CASE 
		WHEN (
				((pln.ctry_cd)::TEXT = ('HK'::CHARACTER VARYING)::TEXT)
				OR ((txn.ctry_cd)::TEXT = ('HK'::CHARACTER VARYING)::TEXT)
				)
			THEN sls.dstr_nm
		WHEN (
				((pln.ctry_cd)::TEXT = ('TW'::CHARACTER VARYING)::TEXT)
				OR ((txn.ctry_cd)::TEXT = ('TW'::CHARACTER VARYING)::TEXT)
				)
			THEN (COALESCE(txn.dstr_nm, ('#'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END AS dstr_nm
	,trim((COALESCE(pln.sls_rep_cd, txn.sls_rep_cd))::TEXT) AS sls_rep_cd
	,pln.sls_rep_typ
	,sls.sls_rep_nm
	,trim((COALESCE(pln.store_cd, txn.store_cd))::TEXT) AS store_cd
	,"k".store_nm
	,COALESCE(pln.store_class, txn.store_class) AS store_class
	,COALESCE(pln.crncy_cd, txn.crncy_cd) AS from_crncy_cd
	,((g.ex_rt)::DOUBLE PRECISION * pln.dly_store_tgt) AS dly_store_tgt
	,CASE 
		WHEN (
				(txn.store_sls_amt IS NOT NULL)
				AND (pln.visit_dt IS NULL)
				)
			THEN 'TNP'::CHARACTER VARYING
		WHEN (
				(txn.store_sls_amt IS NULL)
				AND (pln.visit_dt IS NOT NULL)
				)
			THEN 'PNT'::CHARACTER VARYING
		ELSE 'P&T'::CHARACTER VARYING
		END AS dly_plan_flg
	,txn.prod_cd
	,CASE 
		WHEN (
				(
					(
						(
							((txn.prod_cd)::TEXT like ('1U%'::CHARACTER VARYING)::TEXT)
							OR ((txn.prod_cd)::TEXT like ('COUNTER TOP%'::CHARACTER VARYING)::TEXT)
							)
						OR (txn.prod_cd IS NULL)
						)
					OR ((txn.prod_cd)::TEXT = (''::CHARACTER VARYING)::TEXT)
					)
				OR ((txn.prod_cd)::TEXT like ('DUMPBIN%'::CHARACTER VARYING)::TEXT)
				)
			THEN 'non sellable products'::CHARACTER VARYING
		ELSE 'sellable products'::CHARACTER VARYING
		END AS non_sellable_product
	,CASE 
		WHEN (
				((txn.ctry_cd)::TEXT = ('TW'::CHARACTER VARYING)::TEXT)
				AND (
					(f.prod_hier_l1 IS NULL)
					OR ((f.prod_hier_l1)::TEXT = (''::CHARACTER VARYING)::TEXT)
					)
				)
			THEN 'Taiwan'::CHARACTER VARYING
		WHEN (
				((txn.ctry_cd)::TEXT = ('KR'::CHARACTER VARYING)::TEXT)
				AND (
					(f.prod_hier_l1 IS NULL)
					OR ((f.prod_hier_l1)::TEXT = (''::CHARACTER VARYING)::TEXT)
					)
				)
			THEN 'Korea'::CHARACTER VARYING
		WHEN (
				((txn.ctry_cd)::TEXT = ('HK'::CHARACTER VARYING)::TEXT)
				AND (
					(f.prod_hier_l1 IS NULL)
					OR ((f.prod_hier_l1)::TEXT = (''::CHARACTER VARYING)::TEXT)
					)
				)
			THEN 'HK'::CHARACTER VARYING
		ELSE COALESCE(f.prod_hier_l1, '#'::CHARACTER VARYING)
		END AS prod_hier_l1
	,COALESCE(f.prod_hier_l2, '#'::CHARACTER VARYING) AS prod_hier_l2
	,COALESCE(f.prod_hier_l3, '#'::CHARACTER VARYING) AS prod_hier_l3
	,COALESCE(f.prod_hier_l4, '#'::CHARACTER VARYING) AS prod_hier_l4
	,COALESCE(f.prod_hier_l5, '#'::CHARACTER VARYING) AS prod_hier_l5
	,COALESCE(f.prod_hier_l6, '#'::CHARACTER VARYING) AS prod_hier_l6
	,COALESCE(f.prod_hier_l7, '#'::CHARACTER VARYING) AS prod_hier_l7
	,COALESCE(f.prod_hier_l8, '#'::CHARACTER VARYING) AS prod_hier_l8
	,COALESCE(f.prod_hier_l9, '#'::CHARACTER VARYING) AS prod_hier_l9
	,COALESCE(f1.prod_hier_l9, '#'::CHARACTER VARYING) AS prnt_prod_hier_l9
	,f.sap_matl_num
	,COALESCE(f.lcl_prod_nm, '#'::CHARACTER VARYING) AS lcl_prod_nm
	,txn.msl_flg
	,sum(txn.store_sls_amt) OVER (
		PARTITION BY b.fisc_per
		,pln.visit_dt
		,COALESCE(pln.ctry_cd, txn.ctry_cd)
		,COALESCE(pln.dstr_cd, txn.dstr_cd)
		,COALESCE(pln.sls_rep_cd, txn.sls_rep_cd)
		,COALESCE(pln.store_cd, txn.store_cd)) AS period_sls_amt
	,count(1) OVER (
		PARTITION BY COALESCE(pln.cal_day, txn.ims_txn_dt)
		,COALESCE(pln.ctry_cd, txn.ctry_cd)
		,COALESCE(pln.dstr_cd, txn.dstr_cd)
		,COALESCE(pln.sls_rep_cd, txn.sls_rep_cd)
		,COALESCE(pln.store_cd, txn.store_cd)
		,g.to_crncy 
		) AS dly_prod_cnt
	,txn.store_sls_amt
	,txn.ean_num
	,txn.prnt_ean_num
	,g.to_crncy
	,g.ex_rt
	,(g.ex_rt * txn.sls_amt) AS sls_amt
	,txn.sls_qty
	,txn.rtrn_qty
	,txn.rtrn_amt
	,txn.hq
	,txn.store_type
	,txn.sell_in_price_manual
	,txn.sell_out_unit_price
FROM (
	(
		(
			(
				(
					(
						(
							v_dly_planned_ims pln FULL JOIN v_dly_ims_txn_msl txn ON (
									(
										(
											(
												(
													(
														((pln.ctry_cd)::TEXT = (txn.ctry_cd)::TEXT)
														AND ((pln.dstr_cd)::TEXT = (txn.dstr_cd)::TEXT)
														)
													AND ((pln.store_cd)::TEXT = (txn.store_cd)::TEXT)
													)
												AND ((pln.sls_rep_cd)::TEXT = (txn.sls_rep_cd)::TEXT)
												)
											AND (pln.cal_day = txn.ims_txn_dt)
											)
										AND ((pln.crncy_cd)::TEXT = (txn.crncy_cd)::TEXT)
										)
									)
							) LEFT JOIN v_intrm_calendar_ims b ON ((b.cal_day = COALESCE(pln.cal_day, txn.ims_txn_dt)))
						) LEFT JOIN edw_store_dim "k" ON (
							(
								(
									(("k".dstr_cd)::TEXT = (COALESCE(pln.dstr_cd, txn.dstr_cd))::TEXT)
									AND (("k".store_cd)::TEXT = (COALESCE(pln.store_cd, txn.store_cd))::TEXT)
									)
								AND (("k".ctry_cd)::TEXT = (COALESCE(pln.ctry_cd, txn.ctry_cd))::TEXT)
								)
							)
					) LEFT JOIN (
					SELECT DISTINCT (edw_product_attr_dim.ean)::CHARACTER VARYING(100) AS ean_num
						,edw_product_attr_dim.cntry
						,edw_product_attr_dim.sap_matl_num
						,edw_product_attr_dim.prod_hier_l1
						,edw_product_attr_dim.prod_hier_l2
						,edw_product_attr_dim.prod_hier_l3
						,edw_product_attr_dim.prod_hier_l4
						,edw_product_attr_dim.prod_hier_l5
						,edw_product_attr_dim.prod_hier_l6
						,edw_product_attr_dim.prod_hier_l7
						,edw_product_attr_dim.prod_hier_l8
						,edw_product_attr_dim.prod_hier_l9
						,edw_product_attr_dim.lcl_prod_nm
					FROM edw_product_attr_dim edw_product_attr_dim
					) f ON (
						(
							CASE 
								WHEN (
										(
											((txn.ctry_cd)::TEXT = ('HK'::CHARACTER VARYING)::TEXT)
											AND ((f.ean_num)::TEXT = (txn.ean_num)::TEXT)
											)
										AND ((f.cntry)::TEXT = (txn.ctry_cd)::TEXT)
										)
									THEN 1
								WHEN (
										(
											((txn.ctry_cd)::TEXT = ('TW'::CHARACTER VARYING)::TEXT)
											AND ((f.ean_num)::TEXT = ltrim((txn.ean_num)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
											)
										AND ((f.cntry)::TEXT = (txn.ctry_cd)::TEXT)
										)
									THEN 1
								ELSE 0
								END = 1
							)
						)
				) LEFT JOIN (
				SELECT DISTINCT (edw_product_attr_dim.ean)::CHARACTER VARYING(100) AS ean_num
					,edw_product_attr_dim.cntry
					,edw_product_attr_dim.sap_matl_num
					,edw_product_attr_dim.prod_hier_l1
					,edw_product_attr_dim.prod_hier_l2
					,edw_product_attr_dim.prod_hier_l3
					,edw_product_attr_dim.prod_hier_l4
					,edw_product_attr_dim.prod_hier_l5
					,edw_product_attr_dim.prod_hier_l6
					,edw_product_attr_dim.prod_hier_l7
					,edw_product_attr_dim.prod_hier_l8
					,edw_product_attr_dim.prod_hier_l9
					,edw_product_attr_dim.lcl_prod_nm
				FROM edw_product_attr_dim edw_product_attr_dim
				) f1 ON (
					(
						((f1.ean_num)::TEXT = txn.prnt_ean_num)
						AND ((f1.cntry)::TEXT = (txn.ctry_cd)::TEXT)
						)
					)
			) LEFT JOIN edw_sls_rep_dim sls ON (
				(
					(
						((COALESCE(pln.ctry_cd, txn.ctry_cd))::TEXT = (sls.ctry_cd)::TEXT)
						AND ((COALESCE(pln.dstr_cd, txn.dstr_cd))::TEXT = (sls.dstr_cd)::TEXT)
						)
					AND ((COALESCE(pln.sls_rep_cd, txn.sls_rep_cd))::TEXT = (sls.sls_rep_cd)::TEXT)
					)
				)
		) LEFT JOIN v_intrm_crncy_exch g ON (((COALESCE(pln.crncy_cd, txn.crncy_cd))::TEXT = (g.from_crncy)::TEXT))
	)
GROUP BY pln.cal_day
	,txn.ims_txn_dt
	,b.fisc_per
	,b.cal_wk
	,b.no_of_wks
	,b.fisc_wk_num
	,pln.visit_dt
	,pln.visit_jj_mnth_id
	,pln.visit_jj_wk_no
	,pln.visit_jj_wkdy_no
	,pln.visit_end_dt
	,pln.ctry_cd
	,txn.ctry_cd
	,pln.dstr_cd
	,txn.dstr_cd
	,sls.dstr_nm
	,pln.sls_rep_cd
	,txn.sls_rep_cd
	,pln.sls_rep_typ
	,sls.sls_rep_nm
	,pln.store_cd
	,txn.store_cd
	,"k".store_nm
	,pln.store_class
	,txn.store_class
	,pln.crncy_cd
	,txn.crncy_cd
	,pln.dly_store_tgt
	,CASE 
		WHEN (
				(txn.store_sls_amt IS NOT NULL)
				AND (pln.visit_dt IS NULL)
				)
			THEN 'TNP'::CHARACTER VARYING
		WHEN (
				(txn.store_sls_amt IS NULL)
				AND (pln.visit_dt IS NOT NULL)
				)
			THEN 'PNT'::CHARACTER VARYING
		ELSE 'P&T'::CHARACTER VARYING
		END
	,txn.prod_cd
	,f.prod_hier_l1
	,f.prod_hier_l2
	,f.prod_hier_l3
	,f.prod_hier_l4
	,f.prod_hier_l5
	,f.prod_hier_l6
	,f.prod_hier_l7
	,f.prod_hier_l8
	,f.prod_hier_l9
	,f1.prod_hier_l9
	,f.sap_matl_num
	,f.lcl_prod_nm
	,txn.msl_flg
	,txn.store_sls_amt
	,txn.ean_num
	,txn.prnt_ean_num
	,g.ex_rt
	,g.to_crncy
	,txn.sls_amt
	,txn.sls_qty
	,txn.rtrn_qty
	,txn.rtrn_amt
	,txn.hq
	,txn.store_type
	,txn.sell_in_price_manual
	,txn.sell_out_unit_price
	,txn.dstr_nm
)
select * from final