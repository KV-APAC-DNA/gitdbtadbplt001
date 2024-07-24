with dw_pos_daily as(
    select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.DW_POS_DAILY
),
itg_mds_jp_pos_account_mapping as(
    select * from DEV_DNA_CORE.SNAPJPNITG_INTEGRATION.ITG_MDS_JP_POS_ACCOUNT_MAPPING
),
itg_mds_jp_pos_product_mapping as(
    select * from DEV_DNA_CORE.SNAPJPNITG_INTEGRATION.itg_mds_jp_pos_product_mapping
),
itg_mds_jp_pos_store_mapping as(
    select * from DEV_DNA_CORE.SNAPJPNITG_INTEGRATION.itg_mds_jp_pos_store_mapping
),
edi_store_m as(
    select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.edi_store_m
),
edi_chn_m as(
    select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.edi_chn_m
),
edi_chn_m1 as(
    select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.edi_chn_m1
),
mt_prf as(
    select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.mt_prf
),
mt_cld as(
    select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.mt_cld
),
vw_jan_change as(
    select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.vw_jan_change
),
edi_item_m as(
    select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.edi_item_m
),
vw_m_item_frnch_cdd as(
    select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.vw_m_item_frnch_cdd
),
pos_data_add_date as(
		SELECT pos_data.account_key
			,pos_data.accounting_date
			,pos_data.mdsproductname
			,mt.year_445
			,mt.day
			,mt.half_445
			,mt.quarter_445
			,mt.ymonth_445
			,mt.month_445
			,mt.mweek_445
			,pos_data.jan_code
			,pos_data.mdsaccname
			,frnch.frnch_nm
			,frnch.mjr_prod_nm
			,frnch.mjr_prod_nm2
			,frnch.min_prod_nm
			,item.prom_goods_flg
			,pos_data.category_code
			,pos_data.sub_category_code
			,pos_data.marker_code
			,pos_data.brand_code
			,pos_data.sub_brand_code
			,pos_data.size_id
			,pos_data.size_code
			,pos_data.form_type_code
			,pos_data.oral_function_code
			,pos_data.other_1
			,pos_data.other_2
			,pos_data.other_3
			,pos_data.other_4
			,pos_data.other_5
			,pos_data.planet_store_code
			,pos_data.cmmn_nm_knj
			,pos_data.prf_nm_knj
			,pos_data.chn_cd
			,pos_data.chn_offc_cd
			,pos_data.chn_lgl_nm
			,pos_data.chn1_lgl_nm
			,pos_data.sales_cat_1
			,pos_data.sales_cat_2
			,pos_data.quantity
			,pos_data.amount
			,pos_data.upload_dt
		FROM (
			(
				(
					(
						(
							SELECT pos.store_key_1
								,pos.store_key_2
								,pos.jan_code
								,pos.product_name
								,pos.accounting_date
								,pos.quantity
								,pos.amount
								,pos.account_key
								,pos.source_file_date
								,pos.upload_dt
								,pos.upload_time
								,mdsacct.name AS mdsaccname
								,mdsprt.name AS mdsproductname
								,mdsprt.category_code
								,mdsprt.sub_category_code
								,mdsprt.marker_code
								,mdsprt.brand_code
								,mdsprt.sub_brand_code
								,mdsprt.size_id
								,mdsprt.size_code
								,mdsprt.form_type_code
								,mdsprt.oral_function_code
								,mdsprt.other_1
								,mdsprt.other_2
								,mdsprt.other_3
								,mdsprt.other_4
								,mdsprt.other_5
								,mdsstr.planet_store_code
								,store.cmmn_nm_knj
								,pr.prf_nm_knj
								,chn.chn_cd
								,chn1.lgl_nm AS chn1_lgl_nm
								,chn.chn_offc_cd
								,chn.lgl_nm AS chn_lgl_nm
								,mdsstr.sales_cat_1
								,mdsstr.sales_cat_2
							FROM (
								(
									(
										(
											(
												(
													(
														dw_pos_daily pos JOIN (
															SELECT DISTINCT itg_mds_jp_pos_account_mapping.name
																,itg_mds_jp_pos_account_mapping.code
															FROM itg_mds_jp_pos_account_mapping
															) mdsacct ON (((pos.account_key)::TEXT = (mdsacct.code)::TEXT))
														) JOIN (
														SELECT DISTINCT itg_mds_jp_pos_product_mapping.name
															,itg_mds_jp_pos_product_mapping.category_code
															,itg_mds_jp_pos_product_mapping.sub_category_code
															,itg_mds_jp_pos_product_mapping.marker_code
															,itg_mds_jp_pos_product_mapping.brand_code
															,itg_mds_jp_pos_product_mapping.sub_brand_code
															,itg_mds_jp_pos_product_mapping.size_id
															,itg_mds_jp_pos_product_mapping.size_code
															,itg_mds_jp_pos_product_mapping.form_type_code
															,itg_mds_jp_pos_product_mapping.oral_function_code
															,itg_mds_jp_pos_product_mapping.other_1
															,itg_mds_jp_pos_product_mapping.other_2
															,itg_mds_jp_pos_product_mapping.other_3
															,itg_mds_jp_pos_product_mapping.other_4
															,itg_mds_jp_pos_product_mapping.other_5
															,itg_mds_jp_pos_product_mapping.code
														FROM itg_mds_jp_pos_product_mapping
														) mdsprt ON (((pos.jan_code)::TEXT = (mdsprt.code)::TEXT))
													) LEFT JOIN (
													SELECT DISTINCT itg_mds_jp_pos_store_mapping.planet_store_code
														,itg_mds_jp_pos_store_mapping.sales_cat_1
														,itg_mds_jp_pos_store_mapping.sales_cat_2
														,itg_mds_jp_pos_store_mapping.store_key_1
														,itg_mds_jp_pos_store_mapping.store_key_2
														,itg_mds_jp_pos_store_mapping.account_key
													FROM itg_mds_jp_pos_store_mapping
													) mdsstr ON (
														(
															(
																((pos.account_key)::TEXT = (mdsstr.account_key)::TEXT)
																AND ((pos.store_key_1)::TEXT = (mdsstr.store_key_1)::TEXT)
																)
															AND ((pos.store_key_2)::TEXT = (mdsstr.store_key_2)::TEXT)
															)
														)
												) LEFT JOIN (
												SELECT DISTINCT edi_store_m.cmmn_nm_knj
													,edi_store_m.chn_cd
													,edi_store_m.str_cd
													,edi_store_m.jis_prfct_c
												FROM edi_store_m
												) store ON ((((mdsstr.planet_store_code)::CHARACTER VARYING)::TEXT = (store.str_cd)::TEXT))
											) LEFT JOIN (
											SELECT DISTINCT edi_chn_m.chn_offc_cd
												,edi_chn_m.lgl_nm
												,edi_chn_m.chn_cd
											FROM edi_chn_m
											) chn ON (((store.chn_cd)::TEXT = (chn.chn_cd)::TEXT))
										) LEFT JOIN (
										SELECT DISTINCT edi_chn_m1.lgl_nm
											,edi_chn_m1.chn_cd
										FROM edi_chn_m1
										) chn1 ON (((chn.chn_offc_cd)::TEXT = (chn1.chn_cd)::TEXT))
									) LEFT JOIN mt_prf pr ON (((store.jis_prfct_c)::TEXT = (pr.prf_cd)::TEXT))
								)
							WHERE (
									(pos.quantity <> 0)
									AND (pos.amount <> 0)
									)
							) pos_data LEFT JOIN (
							SELECT DISTINCT mt_cld.ymd_dt
								,mt_cld.year_445
								,mt_cld.day
								,mt_cld.half_445
								,mt_cld.quarter_445
								,mt_cld.ymonth_445
								,mt_cld.month_445
								,mt_cld.mweek_445
							FROM mt_cld
							) mt ON (((pos_data.accounting_date)::TIMESTAMP without TIME zone = mt.ymd_dt))
						) LEFT JOIN (
						SELECT DISTINCT vw_jan_change.jan_cd
							,vw_jan_change.item_cd
						FROM vw_jan_change
						) janchange ON (((pos_data.jan_code)::TEXT = (janchange.jan_cd)::TEXT))
					) LEFT JOIN (
					SELECT DISTINCT edi_item_m.item_cd
						,CASE 
							WHEN (upper((edi_item_m.prom_goods_flg)::TEXT) = 'Y'::TEXT)
								THEN '企画品'::TEXT
							WHEN (upper((edi_item_m.prom_goods_flg)::TEXT) = 'N'::TEXT)
								THEN '通常品'::TEXT
							ELSE NULL::TEXT
							END AS prom_goods_flg
					FROM edi_item_m
					) item ON ((janchange.item_cd = (item.item_cd)::TEXT))
				) LEFT JOIN (
				SELECT DISTINCT vw_m_item_frnch_cdd.item_cd
					,vw_m_item_frnch_cdd.frnch_nm
					,vw_m_item_frnch_cdd.mjr_prod_nm
					,vw_m_item_frnch_cdd.mjr_prod_nm2
					,vw_m_item_frnch_cdd.min_prod_nm
				FROM vw_m_item_frnch_cdd
				) frnch ON ((janchange.item_cd = (frnch.item_cd)::TEXT))
			)
),
pos_data_add_date_dedup as(
	SELECT pos_data_add_date.account_key
		,pos_data_add_date.accounting_date
		,pos_data_add_date.mdsproductname
		,pos_data_add_date.year_445
		,pos_data_add_date.day
		,pos_data_add_date.half_445
		,pos_data_add_date.quarter_445
		,pos_data_add_date.ymonth_445
		,pos_data_add_date.month_445
		,pos_data_add_date.mweek_445
		,pos_data_add_date.jan_code
		,pos_data_add_date.mdsaccname
		,pos_data_add_date.frnch_nm
		,pos_data_add_date.mjr_prod_nm
		,pos_data_add_date.mjr_prod_nm2
		,pos_data_add_date.min_prod_nm
		,pos_data_add_date.prom_goods_flg
		,pos_data_add_date.category_code
		,pos_data_add_date.sub_category_code
		,pos_data_add_date.marker_code
		,pos_data_add_date.brand_code
		,pos_data_add_date.sub_brand_code
		,pos_data_add_date.size_id
		,pos_data_add_date.size_code
		,pos_data_add_date.form_type_code
		,pos_data_add_date.oral_function_code
		,pos_data_add_date.other_1
		,pos_data_add_date.other_2
		,pos_data_add_date.other_3
		,pos_data_add_date.other_4
		,pos_data_add_date.other_5
		,pos_data_add_date.planet_store_code
		,pos_data_add_date.cmmn_nm_knj
		,pos_data_add_date.prf_nm_knj
		,pos_data_add_date.chn_cd
		,pos_data_add_date.chn_offc_cd
		,pos_data_add_date.chn_lgl_nm
		,pos_data_add_date.chn1_lgl_nm
		,pos_data_add_date.sales_cat_1
		,pos_data_add_date.sales_cat_2
		,pos_data_add_date.quantity
		,pos_data_add_date.amount
		,pos_data_add_date.upload_dt
	FROM pos_data_add_date
),
transformed as(
SELECT pos_data_add_date_dedup.account_key
	,pos_data_add_date_dedup.accounting_date
	,pos_data_add_date_dedup.mdsproductname
	,pos_data_add_date_dedup.year_445
	,pos_data_add_date_dedup.day
	,pos_data_add_date_dedup.half_445
	,pos_data_add_date_dedup.quarter_445
	,pos_data_add_date_dedup.ymonth_445
	,pos_data_add_date_dedup.month_445
	,pos_data_add_date_dedup.mweek_445
	,pos_data_add_date_dedup.jan_code
	,pos_data_add_date_dedup.mdsaccname
	,pos_data_add_date_dedup.frnch_nm
	,pos_data_add_date_dedup.mjr_prod_nm
	,pos_data_add_date_dedup.mjr_prod_nm2
	,pos_data_add_date_dedup.min_prod_nm
	,pos_data_add_date_dedup.prom_goods_flg
	,pos_data_add_date_dedup.category_code
	,pos_data_add_date_dedup.sub_category_code
	,pos_data_add_date_dedup.marker_code
	,pos_data_add_date_dedup.brand_code
	,pos_data_add_date_dedup.sub_brand_code
	,pos_data_add_date_dedup.size_id
	,pos_data_add_date_dedup.size_code
	,pos_data_add_date_dedup.form_type_code
	,pos_data_add_date_dedup.oral_function_code
	,pos_data_add_date_dedup.other_1
	,pos_data_add_date_dedup.other_2
	,pos_data_add_date_dedup.other_3
	,pos_data_add_date_dedup.other_4
	,pos_data_add_date_dedup.other_5
	,pos_data_add_date_dedup.planet_store_code
	,pos_data_add_date_dedup.cmmn_nm_knj
	,pos_data_add_date_dedup.prf_nm_knj
	,pos_data_add_date_dedup.chn_cd
	,pos_data_add_date_dedup.chn_offc_cd
	,pos_data_add_date_dedup.chn_lgl_nm
	,pos_data_add_date_dedup.chn1_lgl_nm
	,pos_data_add_date_dedup.sales_cat_1
	,pos_data_add_date_dedup.sales_cat_2
	,pos_data_add_date_dedup.quantity
	,pos_data_add_date_dedup.amount
	,pos_data_add_date_dedup.upload_dt
FROM  pos_data_add_date_dedup
)
select * from transformed