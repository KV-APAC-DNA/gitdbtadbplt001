with edw_material_plant_dim as (
    select * from {{ ref('aspedw_integration__edw_material_plant_dim') }}
),
edw_material_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_material_uom as (
    select * from {{ ref('aspedw_integration__edw_material_uom') }}
),
edw_gch_producthierarchy as 
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
md as(
		SELECT edw_material_dim.matl_num
						,edw_material_dim.matl_desc
						,edw_material_dim.crt_on
						,edw_material_dim.crt_by_nm
						,edw_material_dim.chg_dttm
						,edw_material_dim.chg_by_nm
						,edw_material_dim.maint_sts_cmplt_matl
						,edw_material_dim.maint_sts
						,edw_material_dim.fl_matl_del_clnt_lvl
						,edw_material_dim.matl_type_cd
						,edw_material_dim.indstr_sectr
						,edw_material_dim.matl_grp_cd
						,edw_material_dim.old_matl_num
						,edw_material_dim.base_uom_cd
						,edw_material_dim.prch_uom_cd
						,edw_material_dim.doc_num
						,edw_material_dim.doc_type
						,edw_material_dim.doc_vers
						,edw_material_dim.pg_fmt__doc
						,edw_material_dim.doc_chg_num
						,edw_material_dim.pg_num_doc
						,edw_material_dim.num_sht
						,edw_material_dim.prdtn_memo_txt
						,edw_material_dim.pg_fmt_prdtn_memo
						,edw_material_dim.size_dims_txt
						,edw_material_dim.bsc_matl
						,edw_material_dim.indstr_std_desc
						,edw_material_dim.mercia_plan
						,edw_material_dim.prchsng_val_key
						,edw_material_dim.grs_wt_meas
						,edw_material_dim.net_wt_meas
						,edw_material_dim.wt_uom_cd
						,edw_material_dim.vol_meas
						,edw_material_dim.vol_uom_cd
						,edw_material_dim.cntnr_rqr
						,edw_material_dim.strg_cond
						,edw_material_dim.temp_cond_ind
						,edw_material_dim.low_lvl_cd
						,edw_material_dim.trspn_grp
						,edw_material_dim.haz_matl_num
						,edw_material_dim.div
						,edw_material_dim.cmpt
						,edw_material_dim.ean_obsol
						,edw_material_dim.gr_prtd_qty
						,edw_material_dim.prcmt_rule
						,edw_material_dim.src_supl
						,edw_material_dim.seasn_cat
						,edw_material_dim.lbl_type_cd
						,edw_material_dim.lbl_form
						,edw_material_dim.deact
						,edw_material_dim.prmry_upc_cd
						,edw_material_dim.ean_cat
						,edw_material_dim.lgth_meas
						,edw_material_dim.wdth_meas
						,edw_material_dim.hght_meas
						,edw_material_dim.dim_uom_cd
						,edw_material_dim.prod_hier_cd
						,edw_material_dim.stk_tfr_chg_cost
						,edw_material_dim.cad_ind
						,edw_material_dim.qm_prcmt_act
						,edw_material_dim.allw_pkgng_wt
						,edw_material_dim.wt_unit
						,edw_material_dim.allw_pkgng_vol
						,edw_material_dim.vol_unit
						,edw_material_dim.exces_wt_tlrnc
						,edw_material_dim.exces_vol_tlrnc
						,edw_material_dim.var_prch_ord_unit
						,edw_material_dim.rvsn_lvl_asgn_matl
						,edw_material_dim.configurable_matl_ind
						,edw_material_dim.btch_mgmt_reqt_ind
						,edw_material_dim.pkgng_matl_type_cd
						,edw_material_dim.max_lvl_vol
						,edw_material_dim.stack_fact
						,edw_material_dim.pkgng_matl_grp
						,edw_material_dim.auth_grp
						,edw_material_dim.vld_from_dt
						,edw_material_dim.del_dt
						,edw_material_dim.seasn_yr
						,edw_material_dim.prc_bnd_cat
						,edw_material_dim.bill_of_matl
						,edw_material_dim.extrnl_matl_grp_txt
						,edw_material_dim.cross_plnt_cnfg_matl
						,edw_material_dim.matl_cat
						,edw_material_dim.matl_coprod_ind
						,edw_material_dim.fllp_matl_ind
						,edw_material_dim.prc_ref_matl
						,edw_material_dim.cros_plnt_matl_sts
						,edw_material_dim.cros_dstn_chn_matl_sts
						,edw_material_dim.cros_plnt_matl_sts_vld_dt
						,edw_material_dim.chn_matl_vld_from_dt
						,edw_material_dim.tax_clsn_matl
						,edw_material_dim.catlg_prfl
						,edw_material_dim.min_rmn_shlf_lif
						,edw_material_dim.tot_shlf_lif
						,edw_material_dim.strg_pct
						,edw_material_dim.cntnt_uom_cd
						,edw_material_dim.net_cntnt_meas
						,edw_material_dim.cmpr_prc_unit
						,edw_material_dim.isr_matl_grp
						,edw_material_dim.grs_cntnt_meas
						,edw_material_dim.qty_conv_meth
						,edw_material_dim.intrnl_obj_num
						,edw_material_dim.envmt_rlvnt
						,edw_material_dim.prod_allc_dtrmn_proc
						,edw_material_dim.prc_prfl_vrnt
						,edw_material_dim.matl_qual_disc
						,edw_material_dim.mfr_part_num
						,edw_material_dim.mfr_num
						,edw_material_dim.intrnl_inv_mgmt
						,edw_material_dim.mfr_part_prfl
						,edw_material_dim.meas_usg_unit
						,edw_material_dim.rollout_seasn
						,edw_material_dim.dngrs_goods_ind_prof
						,edw_material_dim.hi_viscous_ind
						,edw_material_dim.in_bulk_lqd_ind
						,edw_material_dim.lvl_explc_ser_num
						,edw_material_dim.pkgng_matl_clse_pkgng
						,edw_material_dim.appr_btch_rec_ind
						,edw_material_dim.ovrd_chg_num
						,edw_material_dim.matl_cmplt_lvl
						,edw_material_dim.per_ind_shlf_lif_expn_dt
						,edw_material_dim.rd_rule_sled
						,edw_material_dim.prod_cmpos_prtd_pkgng
						,edw_material_dim.genl_item_cat_grp
						,edw_material_dim.gn_matl_logl_vrnt
						,edw_material_dim.prod_base
						,edw_material_dim.vrnt
						,edw_material_dim.put_up
						,edw_material_dim.mega_brnd_cd
						,edw_material_dim.brnd_cd
						,edw_material_dim.tech
						,edw_material_dim.color
						,edw_material_dim.seasonality
						,edw_material_dim.mfg_src_cd
						,edw_material_dim.crt_dttm
						,edw_material_dim.updt_dttm
						,edw_material_dim.mega_brnd_desc
						,edw_material_dim.brnd_desc
						,edw_material_dim.varnt_desc
						,edw_material_dim.base_prod_desc
						,edw_material_dim.put_up_desc
						,edw_material_dim.prodh1
						,edw_material_dim.prodh1_txtmd
						,edw_material_dim.prodh2
						,edw_material_dim.prodh2_txtmd
						,edw_material_dim.prodh3
						,edw_material_dim.prodh3_txtmd
						,edw_material_dim.prodh4
						,edw_material_dim.prodh4_txtmd
						,edw_material_dim.prodh5
						,edw_material_dim.prodh5_txtmd
						,edw_material_dim.prodh6
						,edw_material_dim.prodh6_txtmd
						,edw_material_dim.matl_type_desc
					FROM edw_material_dim
					WHERE (
							((edw_material_dim.prod_hier_cd)::TEXT <> ''::TEXT)
							AND (
								ltrim((edw_material_dim.matl_num)::TEXT, (0)::TEXT) IN (
									SELECT DISTINCT ltrim((a.matl_num)::TEXT, (0)::TEXT) AS matl_num
									FROM (
										edw_material_plant_dim a LEFT JOIN (
											SELECT DISTINCT ltrim((a.matl_num)::TEXT, (0)::TEXT) AS matl_num
											FROM edw_material_sales_dim a
											WHERE (
													(
														((a.sls_org)::TEXT = '2000'::TEXT)
														OR ((a.sls_org)::TEXT = '2050'::TEXT)
														)
													AND (
														CASE 
															WHEN ((a.matl_num)::TEXT = ''::TEXT)
																THEN NULL::CHARACTER VARYING
															ELSE a.matl_num
															END IS NOT NULL
														)
													)
											) b ON ((ltrim((a.matl_num)::TEXT, (0)::TEXT) = ltrim(b.matl_num, (0)::TEXT)))
										)
									WHERE (
											(
												((a.plnt)::TEXT = '2000'::TEXT)
												OR ((a.plnt)::TEXT = '2050'::TEXT)
												)
											AND (
												CASE 
													WHEN ((a.matl_num)::TEXT = ''::TEXT)
														THEN NULL::CHARACTER VARYING
													ELSE a.matl_num
													END IS NOT NULL
												)
											)
									)
								)
							)
),
transformed as(
SELECT id.cntry_key  --EDW_VW_OS_MATERIAL_DIM
	,id.sap_matl_num
	,id.sap_mat_desc
	,id.ean_num
	,id.sap_mat_type_cd
	,id.sap_mat_type_desc
	,id.sap_base_uom_cd
	,id.sap_prchse_uom_cd
	,id.sap_prod_sgmt_cd
	,id.sap_prod_sgmt_desc
	,id.sap_base_prod_cd
	,id.sap_base_prod_desc
	,id.sap_mega_brnd_cd
	,id.sap_mega_brnd_desc
	,id.sap_brnd_cd
	,id.sap_brnd_desc
	,id.sap_vrnt_cd
	,id.sap_vrnt_desc
	,id.sap_put_up_cd
	,id.sap_put_up_desc
	,id.sap_grp_frnchse_cd
	,id.sap_grp_frnchse_desc
	,id.sap_frnchse_cd
	,id.sap_frnchse_desc
	,id.sap_prod_frnchse_cd
	,id.sap_prod_frnchse_desc
	,id.sap_prod_mjr_cd
	,id.sap_prod_mjr_desc
	,id.sap_prod_mnr_cd
	,id.sap_prod_mnr_desc
	,id.sap_prod_hier_cd
	,id.sap_prod_hier_desc
	,id.gph_region
	,id.gph_reg_frnchse
	,id.gph_reg_frnchse_grp
	,id.gph_prod_frnchse
	,id.gph_prod_brnd
	,id.gph_prod_sub_brnd
	,id.gph_prod_vrnt
	,id.gph_prod_needstate
	,id.gph_prod_ctgry
	,id.gph_prod_subctgry
	,id.gph_prod_sgmnt
	,id.gph_prod_subsgmnt
	,id.gph_prod_put_up_cd
	,id.gph_prod_put_up_desc
	,id.gph_prod_size
	,id.gph_prod_size_uom
	,id.launch_dt
	,id.qty_shipper_pc
	,id.prft_ctr
	,id.shlf_life
FROM (
	SELECT ('ID'::CHARACTER VARYING)::CHARACTER VARYING(4) AS cntry_key
		,ltrim((md.matl_num)::TEXT, (0)::TEXT) AS sap_matl_num
		,md.matl_desc AS sap_mat_desc
		,sales_dim.ean_num
		,md.matl_type_cd AS sap_mat_type_cd
		,md.matl_type_desc AS sap_mat_type_desc
		,md.base_uom_cd AS sap_base_uom_cd
		,md.prch_uom_cd AS sap_prchse_uom_cd
		,md.prodh1 AS sap_prod_sgmt_cd
		,md.prodh1_txtmd AS sap_prod_sgmt_desc
		,md.prod_base AS sap_base_prod_cd
		,md.base_prod_desc AS sap_base_prod_desc
		,md.mega_brnd_cd AS sap_mega_brnd_cd
		,md.mega_brnd_desc AS sap_mega_brnd_desc
		,md.brnd_cd AS sap_brnd_cd
		,md.brnd_desc AS sap_brnd_desc
		,md.vrnt AS sap_vrnt_cd
		,md.varnt_desc AS sap_vrnt_desc
		,md.put_up AS sap_put_up_cd
		,md.put_up_desc AS sap_put_up_desc
		,md.prodh2 AS sap_grp_frnchse_cd
		,md.prodh2_txtmd AS sap_grp_frnchse_desc
		,md.prodh3 AS sap_frnchse_cd
		,md.prodh3_txtmd AS sap_frnchse_desc
		,md.prodh4 AS sap_prod_frnchse_cd
		,md.prodh4_txtmd AS sap_prod_frnchse_desc
		,md.prodh5 AS sap_prod_mjr_cd
		,md.prodh5_txtmd AS sap_prod_mjr_desc
		,md.prodh5 AS sap_prod_mnr_cd
		,md.prodh5_txtmd AS sap_prod_mnr_desc
		,md.prodh6 AS sap_prod_hier_cd
		,md.prodh6_txtmd AS sap_prod_hier_desc
		,gph."region" AS gph_region
		,gph.regional_franchise AS gph_reg_frnchse
		,gph.regional_franchise_group AS gph_reg_frnchse_grp
		,gph.gcph_franchise AS gph_prod_frnchse
		,gph.gcph_brand AS gph_prod_brnd
		,gph.gcph_subbrand AS gph_prod_sub_brnd
		,gph.gcph_variant AS gph_prod_vrnt
		,gph.gcph_needstate AS gph_prod_needstate
		,gph.gcph_category AS gph_prod_ctgry
		,gph.gcph_subcategory AS gph_prod_subctgry
		,gph.gcph_segment AS gph_prod_sgmnt
		,gph.gcph_subsegment AS gph_prod_subsgmnt
		,gph.put_up_code AS gph_prod_put_up_cd
		,gph.put_up_description AS gph_prod_put_up_desc
		,gph.size AS gph_prod_size
		,gph.unit_of_measure AS gph_prod_size_uom
		,sales_dim.launch_dt
		,(NULL::CHARACTER VARYING)::CHARACTER VARYING(100) AS qty_shipper_pc
		,pd.prft_ctr
		,(ltrim((md.tot_shlf_lif)::TEXT, (0)::TEXT))::CHARACTER VARYING(100) AS shlf_life
	FROM (
		(
			md LEFT JOIN edw_gch_producthierarchy gph ON ((ltrim((md.matl_num)::TEXT, (0)::TEXT) = ltrim((gph.materialnumber)::TEXT, (0)::TEXT)))
				 LEFT JOIN (
				SELECT DISTINCT edw_material_plant_dim.matl_num
					,edw_material_plant_dim.prft_ctr
				FROM edw_material_plant_dim
				WHERE (
						((edw_material_plant_dim.plnt)::TEXT = '2000'::TEXT)
						OR ((edw_material_plant_dim.plnt)::TEXT = '2050'::TEXT)
						)
				) pd ON (
					(
						(ltrim((md.matl_num)::TEXT, (0)::TEXT) = ltrim((pd.matl_num)::TEXT, (0)::TEXT))
						AND (
							CASE 
								WHEN ((pd.matl_num)::TEXT = ''::TEXT)
									THEN NULL::CHARACTER VARYING
								ELSE pd.matl_num
								END IS NOT NULL
							)
						)
					)
			) LEFT JOIN (
			SELECT DISTINCT trim(ltrim((derived_table4.matl_num)::TEXT, '0'::TEXT)) AS matl_num
				,trim(ltrim((derived_table4.ean_num)::TEXT, '0'::TEXT)) AS ean_num
				,derived_table4.sls_org
				,derived_table4.launch_dt
			FROM (
				SELECT derived_table3.matl_num
					,derived_table3.ean_num
					,derived_table3.sls_org
					,derived_table3.launch_dt
				FROM (
					SELECT edw_material_sales_dim.matl_num
						,edw_material_sales_dim.ean_num
						,edw_material_sales_dim.sls_org
						,row_number() OVER (
							PARTITION BY edw_material_sales_dim.matl_num
							,edw_material_sales_dim.ean_num ORDER BY edw_material_sales_dim.matl_num
								,edw_material_sales_dim.ean_num
								,edw_material_sales_dim.sls_org
							) AS rn
						,"max" (edw_material_sales_dim.launch_dt) OVER (
							PARTITION BY edw_material_sales_dim.matl_num
							,edw_material_sales_dim.sls_org order by null ROWS BETWEEN UNBOUNDED PRECEDING
								AND UNBOUNDED FOLLOWING
							) AS launch_dt
					FROM edw_material_sales_dim
					WHERE (
							(
								(
									((edw_material_sales_dim.sls_org)::TEXT = '2000'::TEXT)
									OR ((edw_material_sales_dim.sls_org)::TEXT = '2050'::TEXT)
									)
								AND ((edw_material_sales_dim.ean_num)::TEXT <> ''::TEXT)
								)
							AND (
								CASE 
									WHEN ((edw_material_sales_dim.ean_num)::TEXT = ''::TEXT)
										THEN NULL::CHARACTER VARYING
									ELSE edw_material_sales_dim.ean_num
									END IS NOT NULL
								)
							)
					) derived_table3
				WHERE (derived_table3.rn = 1)
				) derived_table4
			) sales_dim ON ((ltrim(sales_dim.matl_num, (0)::TEXT) = ltrim((md.matl_num)::TEXT, (0)::TEXT)))
		)
	) id
)
select * from transformed