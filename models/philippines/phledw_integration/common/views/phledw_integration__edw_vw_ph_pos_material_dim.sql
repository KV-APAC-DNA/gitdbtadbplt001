with itg_mds_ph_pos_product as(
    select * from DEV_DNA_CORE.PHLITG_INTEGRATION.itg_mds_ph_pos_product
),
transformed as(
    SELECT 'PH' AS cntry_cd
			,'Philippines' AS cntry_nm
			,itg_mds_ph_pos_product.mnth_id AS jj_mnth_id
			,itg_mds_ph_pos_product.cust_cd
			,itg_mds_ph_pos_product.item_cd
			,itg_mds_ph_pos_product.item_nm
			,itg_mds_ph_pos_product.sap_item_cd
			,itg_mds_ph_pos_product.bar_cd
			,itg_mds_ph_pos_product.cust_sku_grp
			,CASE 
				WHEN ((itg_mds_ph_pos_product.cust_cd)::TEXT = ('MDC'::CHARACTER VARYING)::TEXT)
					THEN itg_mds_ph_pos_product.jnj_pc_per_cust_unit
				WHEN ((itg_mds_ph_pos_product.cust_cd)::TEXT = ('PG'::CHARACTER VARYING)::TEXT)
					THEN itg_mds_ph_pos_product.jnj_pc_per_cust_unit
				WHEN ((itg_mds_ph_pos_product.cust_cd)::TEXT = ('WM'::CHARACTER VARYING)::TEXT)
					THEN itg_mds_ph_pos_product.cust_conv_factor
				WHEN ((itg_mds_ph_pos_product.cust_cd)::TEXT = ('ROB'::CHARACTER VARYING)::TEXT)
					THEN itg_mds_ph_pos_product.cust_conv_factor
				WHEN ((itg_mds_ph_pos_product.cust_cd)::TEXT = ('WAT'::CHARACTER VARYING)::TEXT)
					THEN itg_mds_ph_pos_product.cust_conv_factor
				WHEN ((itg_mds_ph_pos_product.cust_cd)::TEXT = ('SS'::CHARACTER VARYING)::TEXT)
					THEN itg_mds_ph_pos_product.jnj_pc_per_cust_unit
				WHEN (
						(
							((itg_mds_ph_pos_product.cust_cd)::TEXT = ('RS'::CHARACTER VARYING)::TEXT)
							OR ((itg_mds_ph_pos_product.cust_cd)::TEXT = ('WC'::CHARACTER VARYING)::TEXT)
							)
						OR ((itg_mds_ph_pos_product.cust_cd)::TEXT = ('SW'::CHARACTER VARYING)::TEXT)
						)
					THEN itg_mds_ph_pos_product.jnj_pc_per_cust_unit
				WHEN ((itg_mds_ph_pos_product.cust_cd)::TEXT = ('DYNA'::CHARACTER VARYING)::TEXT)
					THEN itg_mds_ph_pos_product.cust_conv_factor
				ELSE (NULL::NUMERIC)::NUMERIC(18, 0)
				END AS cust_conv_factor
			,itg_mds_ph_pos_product.cust_item_prc
			,itg_mds_ph_pos_product.lst_period
			,itg_mds_ph_pos_product.early_bk_period
			,NULL::DATE AS eff_str_date
			,NULL::DATE AS eff_end_date
		FROM itg_mds_ph_pos_product
		WHERE ((itg_mds_ph_pos_product.active)::TEXT = ('Y'::CHARACTER VARYING)::TEXT)
)
select * from transformed