with
edw_customer_attr_flat_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
), 
v_interm_cust_hier_dim as
(
    select * from {{ ref('ntaedw_integration__v_interm_cust_hier_dim') }}
),
edw_product_attr_dim as
(
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
), 
v_calendar_dtls as
(
    select * from {{ ref('aspedw_integration__v_calendar_dtls') }}
), 
v_intrm_crncy_exch as
(
    select * from {{ ref('ntaedw_integration__v_intrm_crncy_exch') }}
),
edw_pos_fact as
(
    select * from {{ ref('ntaedw_integration__edw_pos_fact') }}
),
final as
(
    SELECT b.fisc_per
	,b.fisc_wk
	,b.fisc_wk_strt_dt
	,b.fisc_wk_end_dt
	,b.promo_per
	,b.promo_wk
	,b.promo_wk_strt_dt
	,b.promo_wk_end_dt
	,b.univ_per
	,b.univ_wk
	,b.univ_wk_strt_dt
	,b.univ_wk_end_dt
	,sum(a.sls_qty) AS sls_qty
	,sum(a.sls_amt) AS sls_amt_customer
	,sum(a.invnt_qty) AS invnt_qty
	,sum(a.invnt_amt) AS invnt_amt
	,CASE 
		WHEN ((a.ctry_cd)::TEXT = ('TW'::CHARACTER VARYING)::TEXT)
			THEN sum(a.prom_sls_amt)
		ELSE (sum(a.sls_amt))::NUMERIC(18, 0)
		END AS sls_amt
	,a.crncy_cd
	,a.src_sys_cd
	,CASE 
		WHEN ((a.ctry_cd)::TEXT = ('TW'::CHARACTER VARYING)::TEXT)
			THEN 'Taiwan'::CHARACTER VARYING
		WHEN ((a.ctry_cd)::TEXT = ('HK'::CHARACTER VARYING)::TEXT)
			THEN 'Hong Kong'::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END AS ctry_nm
	,CASE 
		WHEN (
				(a.sls_grp IS NULL)
				OR ((a.sls_grp)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN 'Not Available'::CHARACTER VARYING
		ELSE a.sls_grp
		END AS sls_grp
	,CASE 
		WHEN (
				(d.sls_grp_cd IS NULL)
				OR ((d.sls_grp_cd)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN 'Not Available'::CHARACTER VARYING
		ELSE d.sls_grp_cd
		END AS sls_grp_cd
	,CASE 
		WHEN (
				(a.sold_to_party IS NULL)
				OR ((a.sold_to_party)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN 'Not Available'::CHARACTER VARYING
		ELSE a.sold_to_party
		END AS sold_to_party
	,a.ean_num
	,CASE 
		WHEN (
				(a.str_cd IS NULL)
				OR ((a.str_cd)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN 'Not Available'::CHARACTER VARYING
		ELSE a.str_cd
		END AS str_cd
	,CASE 
		WHEN (
				(a.str_nm IS NULL)
				OR ((a.str_nm)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN 'Not Available'::CHARACTER VARYING
		ELSE a.str_nm
		END AS str_nm
	,CASE 
		WHEN (
				(d.store_typ IS NULL)
				OR ((d.store_typ)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN 'Not Available'::CHARACTER VARYING
		ELSE d.store_typ
		END AS store_typ
	,CASE 
		WHEN (
				(a.mysls_brnd_nm IS NULL)
				OR ((a.mysls_brnd_nm)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN 'Not Available'::CHARACTER VARYING
		ELSE a.mysls_brnd_nm
		END AS my_sls_brand_nm
	,CASE 
		WHEN (
				(a.mysls_catg IS NULL)
				OR ((a.mysls_catg)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN 'Not Available'::CHARACTER VARYING
		ELSE a.mysls_catg
		END AS mysls_catg
	,a.vend_prod_cd
	,CASE 
		WHEN (
				(a.matl_num IS NULL)
				OR ((a.matl_num)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN 'Not Available'::CHARACTER VARYING
		ELSE a.matl_num
		END AS matl_num
	,CASE 
		WHEN (
				(a.matl_desc IS NULL)
				OR ((a.matl_desc)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN 'Not Available'::CHARACTER VARYING
		ELSE a.matl_desc
		END AS matl_desc
	,g.to_crncy
	,g.ex_rt_typ
	,g.ex_rt
	,CASE 
		WHEN (
				(d.channel IS NULL)
				OR ((d.channel)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN 'Not Available'::CHARACTER VARYING
		ELSE d.channel
		END AS channel
	,CASE 
		WHEN (
				(e.cust_hier_l1 IS NULL)
				OR ((e.cust_hier_l1)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE e.cust_hier_l1
		END AS cust_hier_l1
	,CASE 
		WHEN (
				(e.cust_hier_l2 IS NULL)
				OR ((e.cust_hier_l2)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE e.cust_hier_l2
		END AS cust_hier_l2
	,CASE 
		WHEN (
				(e.cust_hier_l3 IS NULL)
				OR ((e.cust_hier_l3)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE e.cust_hier_l3
		END AS cust_hier_l3
	,CASE 
		WHEN (
				(e.cust_hier_l4 IS NULL)
				OR ((e.cust_hier_l4)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE e.cust_hier_l4
		END AS cust_hier_l4
	,CASE 
		WHEN (
				(e.cust_hier_l5 IS NULL)
				OR ((e.cust_hier_l5)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE e.cust_hier_l5
		END AS cust_hier_l5
	,CASE 
		WHEN (
				((a.ctry_cd)::TEXT = ('TW'::CHARACTER VARYING)::TEXT)
				AND (
					(f.prod_hier_l1 IS NULL)
					OR ((f.prod_hier_l1)::TEXT = (''::CHARACTER VARYING)::TEXT)
					)
				)
			THEN 'Taiwan'::CHARACTER VARYING
		WHEN (
				((a.ctry_cd)::TEXT = ('HK'::CHARACTER VARYING)::TEXT)
				AND (
					(f.prod_hier_l1 IS NULL)
					OR ((f.prod_hier_l1)::TEXT = (''::CHARACTER VARYING)::TEXT)
					)
				)
			THEN 'HK'::CHARACTER VARYING
		ELSE COALESCE(f.prod_hier_l1, '#'::CHARACTER VARYING)
		END AS prod_hier_l1
	,CASE 
		WHEN (
				(f.prod_hier_l2 IS NULL)
				OR ((f.prod_hier_l2)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE f.prod_hier_l2
		END AS prod_hier_l2
	,CASE 
		WHEN (
				(f.prod_hier_l3 IS NULL)
				OR ((f.prod_hier_l3)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE f.prod_hier_l3
		END AS prod_hier_l3
	,CASE 
		WHEN (
				(f.prod_hier_l4 IS NULL)
				OR ((f.prod_hier_l4)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE f.prod_hier_l4
		END AS prod_hier_l4
	,CASE 
		WHEN (
				(f.prod_hier_l5 IS NULL)
				OR ((f.prod_hier_l5)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE f.prod_hier_l5
		END AS prod_hier_l5
	,CASE 
		WHEN (
				(f.prod_hier_l6 IS NULL)
				OR ((f.prod_hier_l6)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE f.prod_hier_l6
		END AS prod_hier_l6
	,CASE 
		WHEN (
				(f.prod_hier_l7 IS NULL)
				OR ((f.prod_hier_l7)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE f.prod_hier_l7
		END AS prod_hier_l7
	,CASE 
		WHEN (
				(f.prod_hier_l8 IS NULL)
				OR ((f.prod_hier_l8)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE f.prod_hier_l8
		END AS prod_hier_l8
	,CASE 
		WHEN (
				(f.prod_hier_l9 IS NULL)
				OR ((f.prod_hier_l9)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE f.prod_hier_l9
		END AS prod_hier_l9
	,COALESCE(a.prom_prc_amt, (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(16, 5)) AS price
	,CASE 
		WHEN (
				(f.lcl_prod_nm IS NULL)
				OR ((f.lcl_prod_nm)::TEXT = (''::CHARACTER VARYING)::TEXT)
				)
			THEN '#'::CHARACTER VARYING
		ELSE f.lcl_prod_nm
		END AS lcl_prod_nm
FROM (
	(
		(
			(
				(
					(
						SELECT edw_pos_fact.pos_dt
							,edw_pos_fact.vend_cd
							,edw_pos_fact.vend_nm
							,edw_pos_fact.prod_nm
							,edw_pos_fact.vend_prod_cd
							,edw_pos_fact.vend_prod_nm
							,edw_pos_fact.brnd_nm
							,edw_pos_fact.ean_num
							,edw_pos_fact.str_cd
							,edw_pos_fact.str_nm
							,edw_pos_fact.sls_qty
							,edw_pos_fact.sls_amt
							,edw_pos_fact.prom_sls_amt
							,edw_pos_fact.unit_prc_amt
							,edw_pos_fact.invnt_qty
							,edw_pos_fact.invnt_amt
							,edw_pos_fact.invnt_dt
							,edw_pos_fact.crncy_cd
							,edw_pos_fact.src_sys_cd
							,edw_pos_fact.ctry_cd
							,edw_pos_fact.sold_to_party
							,edw_pos_fact.sls_grp
							,edw_pos_fact.mysls_brnd_nm
							,edw_pos_fact.mysls_catg
							,edw_pos_fact.matl_num
							,edw_pos_fact.matl_desc
							,edw_pos_fact.prom_prc_amt
						FROM edw_pos_fact
						WHERE (
								(
									"date_part" (
										year
										,edw_pos_fact.pos_dt
										) >= (
										"date_part" (
											year
											,(current_timestamp()::CHARACTER VARYING)::TIMESTAMP without TIME zone
											) - 3
										)
									)
								AND ((edw_pos_fact.ctry_cd)::TEXT <> ('KR'::CHARACTER VARYING)::TEXT)
								)
						) a LEFT JOIN (
						SELECT DISTINCT edw_customer_attr_flat_dim.store_typ
							,edw_customer_attr_flat_dim.cust_store_ref
							,edw_customer_attr_flat_dim.channel
							,edw_customer_attr_flat_dim.sold_to_party
							,edw_customer_attr_flat_dim.sls_grp_cd
						FROM edw_customer_attr_flat_dim
						) d ON (
							(
								(
									((COALESCE(d.sold_to_party, '~'::CHARACTER VARYING))::TEXT = (COALESCE(a.sold_to_party, '~'::CHARACTER VARYING))::TEXT)
									AND ((COALESCE(d.cust_store_ref, '#'::CHARACTER VARYING))::TEXT = (COALESCE(a.str_cd, '~'::CHARACTER VARYING))::TEXT)
									)
								AND (
									((a.ctry_cd)::TEXT = ('TW'::CHARACTER VARYING)::TEXT)
									OR ((a.ctry_cd)::TEXT = ('HK'::CHARACTER VARYING)::TEXT)
									)
								)
							)
					) LEFT JOIN (
					SELECT DISTINCT v_interm_cust_hier_dim.sold_to_party
						,v_interm_cust_hier_dim.cust_hier_l1
						,v_interm_cust_hier_dim.cust_hier_l2
						,v_interm_cust_hier_dim.cust_hier_l3
						,v_interm_cust_hier_dim.cust_hier_l4
						,v_interm_cust_hier_dim.cust_hier_l5
					FROM v_interm_cust_hier_dim
					) e ON (((COALESCE(e.sold_to_party, '~'::CHARACTER VARYING))::TEXT = (COALESCE(a.sold_to_party, '~'::CHARACTER VARYING))::TEXT))
				) LEFT JOIN (
				SELECT DISTINCT (edw_product_attr_dim.ean)::CHARACTER VARYING(100) AS ean_num
					,edw_product_attr_dim.cntry
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
						((f.ean_num)::TEXT = (a.ean_num)::TEXT)
						AND ((f.cntry)::TEXT = (a.ctry_cd)::TEXT)
						)
					)
			) LEFT JOIN v_calendar_dtls b ON ((a.pos_dt = b.cal_day))
		) LEFT JOIN     v_intrm_crncy_exch g ON (((a.crncy_cd)::TEXT = (g.from_crncy)::TEXT))
	)
GROUP BY b.fisc_per
	,a.crncy_cd
	,a.src_sys_cd
	,a.ctry_cd
	,a.sls_grp
	,d.sls_grp_cd
	,a.sold_to_party
	,a.mysls_brnd_nm
	,g.to_crncy
	,g.ex_rt_typ
	,g.ex_rt
	,d.store_typ
	,b.promo_per
	,b.univ_per
	,b.fisc_wk
	,b.univ_wk
	,b.promo_wk
	,a.mysls_catg
	,a.ean_num
	,a.str_cd
	,a.str_nm
	,a.matl_num
	,a.matl_desc
	,a.vend_prod_cd
	,d.channel
	,e.cust_hier_l1
	,e.cust_hier_l2
	,e.cust_hier_l3
	,e.cust_hier_l4
	,e.cust_hier_l5
	,f.prod_hier_l1
	,f.prod_hier_l2
	,f.prod_hier_l3
	,f.prod_hier_l4
	,f.prod_hier_l5
	,f.prod_hier_l6
	,f.prod_hier_l7
	,f.prod_hier_l8
	,f.prod_hier_l9
	,f.lcl_prod_nm
	,b.fisc_wk_strt_dt
	,b.fisc_wk_end_dt
	,b.promo_wk_strt_dt
	,b.promo_wk_end_dt
	,b.univ_wk_strt_dt
	,b.univ_wk_end_dt
	,a.prom_prc_amt
)
select * from final