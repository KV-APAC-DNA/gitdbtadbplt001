{{ config(
    materialized = "incremental", 
    incremental_strategy = "append", 
    pre_hook = 
		"delete from {{this}} where UPPER(CUST_CD) ||JJ_MNTH_ID||LTRIM(BRNCH_CD,'0') ||LTRIM(ITEM_CD,'0')  in (
          select distinct upper(substring(file_nm,1,3)) ||jj_mnth_id||ltrim(store_cd,'0') ||ltrim(pos_prod_cd,'0') from {{ ref('phlwks_integration__wks_ph_pos_robinsons') }} 
          union 
          select distinct upper(substring(file_nm,1,3)) ||jj_mnth_id||ltrim(store_cd,'0') ||nvl(ltrim(pos_prod_cd,'0'),'') from {{ ref('phlwks_integration__wks_ph_pos_mercury') }} 
          union 
          select distinct upper(substring(file_nm,1,2)) ||jj_mnth_id||ltrim(store_cd,'0') ||ltrim(pos_prod_cd,'0') from {{ ref('phlwks_integration__wks_ph_pos_rustans') }} 
          union 
          select distinct upper(substring(file_nm,1,2)) ||jj_mnth_id||ltrim(store_cd,'0') ||ltrim(pos_prod_cd,'0') from {{ ref('phlwks_integration__wks_ph_pos_south_star') }} 
          union 
          select distinct upper(substring(file_nm,1,3)) ||jj_mnth_id||ltrim(store_cd,'0') ||ltrim(pos_prod_cd,'0') from {{ ref('phlwks_integration__wks_ph_pos_watsons') }} 
          union 
          select distinct 'DYNA'||mnth_id||customer_id||matl_num from {{ ref('phlwks_integration__wks_ph_pos_dyna_sales') }} 
          union 
          select distinct  'PSC' ||MNTH_ID||STORE_CD||ITEM_CD  from {{ ref('phlwks_integration__wks_ph_pos_711') }}
          union 
          select distinct UPPER(SUBSTRING(FILE_NM,1,2)) ||JJ_MNTH_ID||LTRIM(STORE_CD,'0') ||LTRIM(POS_PROD_CD,'0') from {{ ref('phlwks_integration__wks_ph_pos_waltermart') }} WHERE VENDOR_CD = '6256'
         );"
	) }}
WITH itg_mds_ph_pos_pricelist AS (
		SELECT *
		FROM {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
		),
	itg_mds_ph_pos_product AS (
		SELECT *
		FROM {{ ref('phlitg_integration__itg_mds_ph_pos_product') }}
		),
	sdl_ph_pos_robinsons AS (
		SELECT *
		FROM {{ ref('phlwks_integration__wks_ph_pos_robinsons') }}
		),
	sdl_ph_pos_mercury AS (
		SELECT *
		FROM {{ ref('phlwks_integration__wks_ph_pos_mercury') }}
		),
	sdl_ph_pos_rustans AS (
		SELECT *
		FROM {{ ref('phlwks_integration__wks_ph_pos_rustans') }}
		),
	sdl_ph_pos_south_star AS (
		SELECT *
		FROM {{ ref('phlwks_integration__wks_ph_pos_south_star') }}
		),
	sdl_ph_pos_waltermart AS (
		SELECT *
		FROM {{ ref('phlwks_integration__wks_ph_pos_waltermart') }}
		),
	sdl_ph_pos_watsons AS (
		SELECT *
		FROM {{ ref('phlwks_integration__wks_ph_pos_watsons') }}
		),
	sdl_ph_pos_dyna_sales AS (
		SELECT *
		FROM {{ ref('phlwks_integration__wks_ph_pos_dyna_sales') }}
		),
	sdl_ph_pos_711 AS (
		SELECT *
		FROM {{ ref('phlwks_integration__wks_ph_pos_711') }}
		),
	itg_mds_ph_pos_pricelist AS (
		SELECT *
		FROM {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
		),
	itg_ph_pricelist AS (
		SELECT *
		FROM {{ ref('phlitg_integration__itg_ph_pricelist') }}
		),
	itg_ph_711_product_dim AS (
		SELECT *
		FROM {{ref('phlitg_integration__itg_ph_711_product_dim') }}
		),
	itg_ph_dyna_product_dim AS (
		SELECT *
		FROM {{ref('phlitg_integration__itg_ph_dyna_product_dim') }}
		),
	robinsons AS (
		SELECT upper(substring(ippd.file_nm, 1, 3)) AS cust_cd,
			ippd.jj_mnth_id AS jj_mnth_id,
			ippd.pos_prod_cd AS item_cd,
			ippd.store_cd AS brnch_cd,
			cast(ippd.qty AS NUMERIC(15, 4)) AS pos_qty,
			cast(ippd.amt AS NUMERIC(15, 4)) AS pos_gts,
			cast(ippd.cust_price_pc AS NUMERIC(15, 4)) AS pos_item_prc,
			cast(ippd.tax_amt AS NUMERIC(15, 4)) AS pos_tax,
			cast(ippd.net_amt AS NUMERIC(15, 4)) AS pos_nts,
			cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4)) AS conv_factor,
			cast(ippd.qty AS NUMERIC(15, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4)) AS jj_qty_pc,
			ipp2.lst_price_unit AS jj_item_prc_per_pc,
			(cast(ippd.qty AS NUMERIC(15, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4))) * ipp2.lst_price_unit AS jj_gts,
			(cast(ippd.qty AS NUMERIC(15, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4)) * ipp2.lst_price_unit) * (12.0 / 112.0) AS jj_vat_amt,
			(cast(ippd.qty AS NUMERIC(15, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4)) * ipp2.lst_price_unit) * (100.0 / 112.0) AS jj_nts,
			ippd.file_nm,
			ippd.cdl_dttm,
			current_timestamp() AS crtd_dttm,
			NULL AS updt_dttm
		FROM (
			SELECT *
			FROM itg_mds_ph_pos_pricelist
			WHERE active = 'Y'
			) AS ipp2,
			(
				SELECT sales.cust_item_cd,
					cust_cd,
					sap_item_cd,
					cust_conv_factor,
					jnj_pc_per_cust_unit,
					sales.jj_mnth_id AS jj_mnth_id,
					CASE 
						WHEN sales.mnth_id = ipp.jj_mnth_id
							THEN ipp.jj_mnth_id
						WHEN sales.mnth_id != nvl(ipp.jj_mnth_id, '0')
							AND sales.early_bk_period != ''
							AND upper(sales.early_bk_period) != 'NULL'
							THEN sales.early_bk_period
						WHEN sales.mnth_id != nvl(ipp.jj_mnth_id, '0')
							AND (
								sales.early_bk_period IS NULL
								OR upper(sales.early_bk_period) = 'NULL'
								)
							THEN sales.lst_period
						END AS pl_jj_mnth_id,
					sales.pos_prod_cd,
					sales.store_cd,
					sales.qty,
					sales.amt,
					sales.cust_price_pc,
					sales.tax_amt,
					sales.net_amt,
					sales.file_nm,
					sales.cdl_dttm
				FROM (
					SELECT *
					FROM (
						SELECT DISTINCT mnth_id,
							item_cd AS cust_item_cd,
							cust_cd,
							sap_item_cd,
							cust_conv_factor,
							jnj_pc_per_cust_unit,
							lst_period,
							early_bk_period
						FROM itg_mds_ph_pos_product
						WHERE active = 'Y'
							AND upper(cust_cd) = 'ROB'
						) ipppd,
						sdl_ph_pos_robinsons spm
					WHERE upper(ltrim(ipppd.cust_item_cd(+), '0')) = upper(ltrim(spm.pos_prod_cd, '0'))
						AND ipppd.mnth_id(+) = spm.jj_mnth_id
					) AS sales,
					(
						SELECT *
						FROM itg_mds_ph_pos_pricelist
						WHERE active = 'Y'
						) AS ipp
				WHERE ipp.jj_mnth_id(+) = sales.jj_mnth_id
					AND ipp.item_cd(+) = sales.sap_item_cd
				) AS ippd
		WHERE upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)
			AND trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
		),
	mercury AS (
		SELECT upper(substring(ippd.file_nm, 1, 3)) AS cust_cd,
			ippd.jj_mnth_id AS jj_mnth_id,
			ippd.pos_prod_cd AS item_cd,
			ippd.store_cd AS brnch_cd,
			cast(ippd.qty AS NUMERIC(20, 4)) AS pos_qty,
			NULL AS pos_gts,
			NULL AS pos_item_prc,
			NULL AS pos_tax,
			NULL AS pos_nts,
			cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4)) AS conv_factor,
			cast(ippd.qty AS NUMERIC(20, 4)) * cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4)) AS jj_qty_pc,
			cast(ipp2.lst_price_unit AS NUMERIC(20, 4)) AS jj_item_prc_per_pc,
			(cast(ippd.qty AS NUMERIC(20, 4)) * cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4))) * cast(ipp2.lst_price_unit AS NUMERIC(20, 4)) AS jj_gts,
			((cast(ippd.qty AS NUMERIC(20, 4)) * cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4))) * cast(ipp2.lst_price_unit AS NUMERIC(20, 4))) * (12.0 / 112.0) AS jj_vat_amt,
			((cast(ippd.qty AS NUMERIC(20, 4)) * cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4))) * cast(ipp2.lst_price_unit AS NUMERIC(20, 4))) * (100.0 / 112.0) AS jj_nts,
			ippd.file_nm,
			ippd.cdl_dttm,
			current_timestamp() AS crtd_dttm,
			NULL AS updt_dttm
		FROM (
			SELECT *
			FROM itg_mds_ph_pos_pricelist
			WHERE active = 'Y'
			) AS ipp2,
			(
				SELECT sales.cust_item_cd,
					'MDC' AS cust_cd,
					sap_item_cd,
					cust_conv_factor,
					jnj_pc_per_cust_unit,
					sales.jj_mnth_id AS jj_mnth_id,
					CASE 
						WHEN sales.mnth_id = ipp.jj_mnth_id
							THEN ipp.jj_mnth_id
						WHEN sales.mnth_id != nvl(ipp.jj_mnth_id, '0')
							AND sales.early_bk_period != ''
							AND upper(sales.early_bk_period) != 'NULL'
							THEN sales.early_bk_period
						WHEN sales.mnth_id != nvl(ipp.jj_mnth_id, '0')
							AND (
								sales.early_bk_period IS NULL
								OR upper(sales.early_bk_period) = 'NULL'
								)
							THEN sales.lst_period
						END AS pl_jj_mnth_id,
					sales.pos_prod_cd,
					sales.store_cd,
					sales.qty,
					sales.file_nm,
					sales.cdl_dttm
				FROM (
					SELECT *
					FROM (
						SELECT DISTINCT mnth_id,
							item_cd AS cust_item_cd,
							cust_cd,
							sap_item_cd,
							cust_conv_factor,
							jnj_pc_per_cust_unit,
							lst_period,
							early_bk_period
						FROM itg_mds_ph_pos_product
						WHERE active = 'Y'
							AND upper(cust_cd) = 'MDC'
						) ipppd,
						sdl_ph_pos_mercury spm
					WHERE upper(trim(ipppd.cust_item_cd(+))) = upper(trim(spm.pos_prod_cd))
						AND ipppd.mnth_id(+) = spm.jj_mnth_id
					) AS sales,
					(
						SELECT *
						FROM itg_mds_ph_pos_pricelist
						WHERE active = 'Y'
						) AS ipp
				WHERE ipp.jj_mnth_id(+) = sales.jj_mnth_id
					AND ipp.item_cd(+) = sales.sap_item_cd
				) AS ippd
		WHERE upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)
			AND trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
		),
	rustans AS (
		(
			SELECT upper(substring(ippd.file_nm, 1, 2)) AS cust_cd,
				ippd.jj_mnth_id AS jj_mnth_id,
				ippd.pos_prod_cd AS item_cd,
				ippd.store_cd AS brnch_cd,
				cast(ippd.qty AS NUMERIC(15, 4)) AS pos_qty,
				cast(ippd.amt AS NUMERIC(15, 4)) AS pos_gts,
				CASE 
					WHEN cast(ippd.qty AS NUMERIC(15, 4)) <> 0
						THEN cast(ippd.amt AS NUMERIC(15, 4)) / cast(ippd.qty AS NUMERIC(15, 4))
					ELSE 0
					END AS pos_item_prc,
				cast(ippd.amt AS NUMERIC(15, 4)) * (12.0 / 112.0) AS pos_tax,
				cast(ippd.amt AS NUMERIC(15, 4)) * (100.0 / 112.0) AS pos_nts,
				cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4)) AS conv_factor,
				cast(ippd.qty AS NUMERIC(15, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4)) AS jj_qty_pc,
				ipp2.lst_price_unit AS jj_item_prc_per_pc,
				(cast(ippd.qty AS NUMERIC(15, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4))) * ipp2.lst_price_unit AS jj_gts,
				((cast(ippd.qty AS NUMERIC(15, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4))) * ipp2.lst_price_unit) * (12.0 / 112.0) AS jj_vat_amt,
				((cast(ippd.qty AS NUMERIC(15, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4))) * ipp2.lst_price_unit) * (100.0 / 112.0) AS jj_nts,
				ippd.file_nm,
				ippd.cdl_dttm,
				current_timestamp() AS crtd_dttm,
				NULL AS updt_dttm
			FROM (
				SELECT *
				FROM itg_mds_ph_pos_pricelist
				WHERE active = 'Y'
				) AS ipp2,
				(
					SELECT sales.cust_item_cd,
						cust_cd,
						sap_item_cd,
						cust_conv_factor,
						jnj_pc_per_cust_unit,
						cust_item_prc,
						sales.jj_mnth_id,
						CASE 
							WHEN sales.mnth_id = ipp.jj_mnth_id
								THEN ipp.jj_mnth_id
							WHEN sales.mnth_id != nvl(ipp.jj_mnth_id, '0')
								AND sales.early_bk_period != ''
								AND upper(sales.early_bk_period) != 'NULL'
								THEN sales.early_bk_period
							WHEN sales.mnth_id != nvl(ipp.jj_mnth_id, '0')
								AND (
									sales.early_bk_period IS NULL
									OR upper(sales.early_bk_period) = 'NULL'
									)
								THEN sales.lst_period
							END AS pl_jj_mnth_id,
						sales.pos_prod_cd,
						sales.store_cd,
						sales.qty,
						sales.amt,
						sales.file_nm,
						sales.cdl_dttm
					FROM (
						SELECT *
						FROM (
							SELECT DISTINCT mnth_id,
								item_cd AS cust_item_cd,
								cust_cd,
								sap_item_cd,
								cust_conv_factor,
								cust_item_prc,
								jnj_pc_per_cust_unit,
								lst_period,
								early_bk_period
							FROM itg_mds_ph_pos_product
							WHERE active = 'Y'
								AND upper(cust_cd) IN ('RS', 'WC', 'SW')
							) ipppd,
							sdl_ph_pos_rustans spm
						WHERE upper(trim(ipppd.cust_item_cd(+))) = upper(trim(spm.pos_prod_cd))
							AND ipppd.mnth_id(+) = spm.jj_mnth_id
							AND ipppd.cust_cd(+) = upper(substring(file_nm, 1, 2))
						) AS sales,
						(
							SELECT *
							FROM itg_mds_ph_pos_pricelist
							WHERE active = 'Y'
							) AS ipp
					WHERE ipp.jj_mnth_id(+) = sales.jj_mnth_id
						AND ipp.item_cd(+) = sales.sap_item_cd
					) AS ippd
			WHERE upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)
				AND trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
			)
		),
	southstar AS (
		SELECT upper(substring(ippd.file_nm, 1, 2)) AS cust_cd,
			ippd.jj_mnth_id AS jj_mnth_id,
			ippd.pos_prod_cd AS item_cd,
			ippd.store_cd AS brnch_cd,
			cast(ippd.qty AS NUMERIC(20, 2)) AS pos_qty,
			cast(ippd.amt AS NUMERIC(15, 4)) AS pos_gts,
			CASE 
				WHEN cast(ippd.qty AS NUMERIC(15, 4)) <> 0
					THEN cast(ippd.amt AS NUMERIC(15, 4)) / cast(ippd.qty AS NUMERIC(15, 4))
				ELSE 0
				END AS pos_item_prc,
			cast(ippd.amt AS NUMERIC(15, 4)) * (12.0 / 112.0) AS pos_tax,
			cast(ippd.amt AS NUMERIC(15, 4)) * (100.0 / 112.0) AS pos_nts,
			cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4)) AS conv_factor,
			cast(ippd.qty AS NUMERIC(15, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4)) AS jj_qty_pc,
			ipp2.lst_price_unit AS jj_item_prc_per_pc,
			(cast(ippd.qty AS NUMERIC(15, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4))) * ipp2.lst_price_unit AS jj_gts,
			((cast(ippd.qty AS NUMERIC(15, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4))) * ipp2.lst_price_unit) * (12.0 / 112.0) AS jj_vat_amt,
			(cast(ippd.qty AS NUMERIC(15, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(15, 4)) * ipp2.lst_price_unit) * (100.0 / 112.0) AS jj_nts,
			ippd.file_nm,
			ippd.cdl_dttm,
			current_timestamp() AS crtd_dttm,
			NULL AS updt_dttm
		FROM (
			SELECT *
			FROM itg_mds_ph_pos_pricelist
			WHERE active = 'Y'
			) AS ipp2,
			(
				SELECT sales.cust_item_cd,
					cust_cd,
					sap_item_cd,
					cust_conv_factor,
					jnj_pc_per_cust_unit,
					cust_item_prc,
					sales.jj_mnth_id,
					CASE 
						WHEN sales.mnth_id = ipp.jj_mnth_id
							THEN ipp.jj_mnth_id
						WHEN sales.mnth_id != nvl(ipp.jj_mnth_id, '0')
							AND sales.early_bk_period != ''
							AND upper(sales.early_bk_period) != 'NULL'
							THEN sales.early_bk_period
						WHEN sales.mnth_id != nvl(ipp.jj_mnth_id, '0')
							AND (
								sales.early_bk_period IS NULL
								OR upper(sales.early_bk_period) = 'NULL'
								)
							THEN sales.lst_period
						END AS pl_jj_mnth_id,
					sales.pos_prod_cd,
					sales.store_cd,
					sales.qty,
					sales.amt,
					sales.file_nm,
					sales.cdl_dttm
				FROM (
					SELECT *
					FROM (
						SELECT DISTINCT mnth_id,
							item_cd AS cust_item_cd,
							cust_cd,
							sap_item_cd,
							cust_conv_factor,
							jnj_pc_per_cust_unit,
							cust_item_prc,
							lst_period,
							early_bk_period
						FROM itg_mds_ph_pos_product
						WHERE active = 'Y'
							AND upper(cust_cd) = 'SS'
						) ipppd,
						sdl_ph_pos_south_star spm
					WHERE upper(trim(ipppd.cust_item_cd(+))) = upper(trim(spm.pos_prod_cd))
						AND ipppd.mnth_id(+) = spm.jj_mnth_id
					) AS sales,
					(
						SELECT *
						FROM itg_mds_ph_pos_pricelist
						WHERE active = 'Y'
						) AS ipp
				WHERE ipp.jj_mnth_id(+) = sales.jj_mnth_id
					AND ipp.item_cd(+) = sales.sap_item_cd
				) AS ippd
		WHERE upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)
			AND trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
		),
	waltermart AS (
		SELECT upper(substring(ippd.file_nm, 1, 2)) AS cust_cd,
			ippd.jj_mnth_id AS jj_mnth_id,
			ippd.pos_prod_cd AS item_cd,
			ippd.store_cd AS brnch_cd,
			cast(ippd.qty AS NUMERIC(20, 4)) AS pos_qty,
			cast(ippd.amt AS NUMERIC(20, 4)) AS pos_gts,
			cast(ippd.amt AS NUMERIC(20, 4)) / nullif(cast(ippd.qty AS NUMERIC(20, 4)), 0) AS pos_item_prc,
			cast(ippd.amt AS NUMERIC(20, 4)) * (12.0 / 112.0) AS pos_tax,
			cast(ippd.amt AS NUMERIC(20, 4)) * (100.0 / 112.0) AS pos_nts,
			cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4)) AS conv_factor,
			cast(ippd.qty AS NUMERIC(20, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4)) AS jj_qty_pc,
			cast(ipp2.lst_price_unit AS NUMERIC(20, 4)) AS jj_item_prc_per_pc,
			(cast(ippd.qty AS NUMERIC(20, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4))) * cast(ipp2.lst_price_unit AS NUMERIC(20, 4)) AS jj_gts,
			((cast(ippd.qty AS NUMERIC(20, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4))) * cast(ipp2.lst_price_unit AS NUMERIC(20, 4))) * (12.0 / 112.0) AS jj_vat_amt,
			(cast(ippd.qty AS NUMERIC(20, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4))) * cast(ipp2.lst_price_unit AS NUMERIC(20, 4)) * (100.0 / 112.0) AS jj_nts,
			ippd.file_nm,
			ippd.cdl_dttm,
			current_timestamp() AS crtd_dttm,
			NULL AS updt_dttm
		FROM (
			SELECT *
			FROM itg_mds_ph_pos_pricelist
			WHERE active = 'Y'
			) AS ipp2,
			(
				SELECT sales.cust_item_cd,
					cust_cd,
					sap_item_cd,
					cust_conv_factor,
					jnj_pc_per_cust_unit,
					sales.jj_mnth_id,
					CASE 
						WHEN sales.mnth_id = ipp.jj_mnth_id
							THEN ipp.jj_mnth_id
						WHEN sales.mnth_id != nvl(ipp.jj_mnth_id, '0')
							AND sales.early_bk_period != ''
							AND upper(sales.early_bk_period) != 'NULL'
							THEN sales.early_bk_period
						WHEN sales.mnth_id != nvl(ipp.jj_mnth_id, '0')
							AND (
								sales.early_bk_period IS NULL
								OR upper(sales.early_bk_period) = 'NULL'
								)
							THEN sales.lst_period
						END AS pl_jj_mnth_id,
					sales.pos_prod_cd,
					sales.store_cd,
					sales.qty,
					sales.amt,
					sales.file_nm,
					sales.cdl_dttm
				FROM (
					SELECT *
					FROM (
						SELECT DISTINCT mnth_id,
							item_cd AS cust_item_cd,
							cust_cd,
							sap_item_cd,
							cust_conv_factor,
							jnj_pc_per_cust_unit,
							lst_period,
							early_bk_period
						FROM itg_mds_ph_pos_product
						WHERE active = 'Y'
							AND upper(cust_cd) = 'WM'
						) ipppd,
						(
							SELECT *
							FROM sdl_ph_pos_waltermart
							WHERE vendor_cd = '6256'
								AND qty NOT LIKE '0.000%'
								AND amt NOT LIKE '0.000%'
							) spm
					WHERE upper(trim(ipppd.cust_item_cd(+))) = upper(trim(spm.pos_prod_cd))
						AND ipppd.mnth_id(+) = spm.jj_mnth_id
					) AS sales,
					(
						SELECT *
						FROM itg_mds_ph_pos_pricelist
						WHERE active = 'Y'
						) AS ipp
				WHERE ipp.jj_mnth_id(+) = sales.jj_mnth_id
					AND ipp.item_cd(+) = sales.sap_item_cd
				) AS ippd
		WHERE upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)
			AND trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
		),
	watsons AS (
		SELECT upper(substring(ippd.file_nm, 1, 3)) AS cust_cd,
			ippd.jj_mnth_id AS jj_mnth_id,
			ippd.pos_prod_cd AS item_cd,
			ippd.store_cd AS brnch_cd,
			cast(ippd.qty AS NUMERIC(20, 4)) AS pos_qty,
			cast(ippd.amt AS NUMERIC(20, 4)) AS pos_gts,
			cast(ippd.amt AS NUMERIC(20, 4)) / nullif(cast(ippd.qty AS NUMERIC(20, 4)), 0) AS pos_item_prc,
			cast(ippd.amt AS NUMERIC(20, 4)) * (12.0 / 112.0) AS pos_tax,
			cast(ippd.amt AS NUMERIC(20, 4)) * (100.0 / 112.0) AS pos_nts,
			cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4)) AS conv_factor,
			cast(ippd.qty AS NUMERIC(20, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4)) AS jj_qty_pc,
			cast(ipp2.lst_price_unit AS NUMERIC(20, 4)) AS jj_item_prc_per_pc,
			(cast(ippd.qty AS NUMERIC(20, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4))) * cast(ipp2.lst_price_unit AS NUMERIC(20, 4)) AS jj_gts,
			((cast(ippd.qty AS NUMERIC(20, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4))) * cast(ipp2.lst_price_unit AS NUMERIC(20, 4))) * (12.0 / 112.0) AS jj_vat_amt,
			((cast(ippd.qty AS NUMERIC(20, 4)) / cast(ippd.jnj_pc_per_cust_unit AS NUMERIC(20, 4))) * cast(ipp2.lst_price_unit AS NUMERIC(20, 4))) * (100.0 / 112.0) AS jj_nts,
			ippd.file_nm,
			ippd.cdl_dttm,
			current_timestamp() AS crtd_dttm,
			NULL AS updt_dttm
		FROM (
			SELECT *
			FROM itg_mds_ph_pos_pricelist
			WHERE active = 'Y'
			) AS ipp2,
			(
				SELECT sales.cust_item_cd,
					cust_cd,
					sap_item_cd,
					cust_conv_factor,
					jnj_pc_per_cust_unit,
					sales.jj_mnth_id,
					CASE 
						WHEN sales.mnth_id = ipp.jj_mnth_id
							THEN ipp.jj_mnth_id
						WHEN sales.mnth_id != nvl(ipp.jj_mnth_id, '0')
							AND sales.early_bk_period != ''
							AND upper(sales.early_bk_period) != 'NULL'
							THEN sales.early_bk_period
						WHEN sales.mnth_id != nvl(ipp.jj_mnth_id, '0')
							AND (
								sales.early_bk_period IS NULL
								OR upper(sales.early_bk_period) = 'NULL'
								)
							THEN sales.lst_period
						END AS pl_jj_mnth_id,
					sales.pos_prod_cd,
					sales.store_cd,
					sales.qty,
					sales.amt,
					sales.file_nm,
					sales.cdl_dttm
				FROM (
					SELECT *
					FROM (
						SELECT DISTINCT mnth_id,
							item_cd AS cust_item_cd,
							cust_cd,
							sap_item_cd,
							cust_conv_factor,
							jnj_pc_per_cust_unit,
							lst_period,
							early_bk_period
						FROM itg_mds_ph_pos_product
						WHERE active = 'Y'
							AND upper(cust_cd) = 'WAT'
						) ipppd,
						sdl_ph_pos_watsons spm
					WHERE upper(trim(ipppd.cust_item_cd(+))) = upper(trim(spm.pos_prod_cd))
						AND ipppd.mnth_id(+) = spm.jj_mnth_id
					) AS sales,
					(
						SELECT *
						FROM itg_mds_ph_pos_pricelist
						WHERE active = 'Y'
						) AS ipp
				WHERE ipp.jj_mnth_id(+) = sales.jj_mnth_id
					AND ipp.item_cd(+) = sales.sap_item_cd
				) AS ippd
		WHERE upper(trim(ipp2.item_cd(+))) = upper(ippd.sap_item_cd)
			AND trim(ipp2.jj_mnth_id(+)) = trim(ippd.pl_jj_mnth_id)
		),
	seveneleven as
    (   
    SELECT CUST_CD,
        IPPD.JJ_MNTH_ID AS JJ_MNTH_ID,
        IPPD.POS_PROD_CD AS ITEM_CD,
        IPPD.STORE_CD AS BRNCH_CD,
        CAST(IPPD.TOT_QTY AS NUMERIC(15, 4)) AS POS_QTY,
        CAST(IPPD.TOT_AMT AS NUMERIC(15, 4)) AS POS_GTS,
        NULL AS POS_ITEM_PRC,
        CAST(IPPD.TOT_AMT AS NUMERIC(15, 4)) * (12.0 / 112.0)::float AS POS_TAX,
        CAST(IPPD.TOT_AMT AS NUMERIC(15, 4)) * (100.0 / 112.0)::float AS POS_NTS,
        CAST(IPPD.JNJ_PC_PER_CUST_UNIT AS NUMERIC(15, 4)) AS CONV_FACTOR,
        CAST(IPPD.TOT_QTY AS NUMERIC(15, 4)) / CAST(IPPD.JNJ_PC_PER_CUST_UNIT AS NUMERIC(15, 4)) AS JJ_QTY_PC,
        IPP2.LST_PRICE_UNIT AS JJ_ITEM_PRC_PER_PC,
        CAST(IPPD.TOT_QTY AS NUMERIC(15, 4)) / CAST(IPPD.JNJ_PC_PER_CUST_UNIT AS NUMERIC(15, 4)) * IPP2.LST_PRICE_UNIT AS JJ_GTS,
        (
            CAST(IPPD.TOT_QTY AS NUMERIC(15, 4)) / CAST(IPPD.JNJ_PC_PER_CUST_UNIT AS NUMERIC(15, 4)) * IPP2.LST_PRICE_UNIT
        ) * (12.0 / 112.0)::float AS JJ_VAT_AMT,
        (
            CAST(IPPD.TOT_QTY AS NUMERIC(15, 4)) / CAST(IPPD.JNJ_PC_PER_CUST_UNIT AS NUMERIC(15, 4)) * IPP2.LST_PRICE_UNIT
        ) * (100.0 / 112.0)::float AS JJ_NTS,
        NULL AS FILE_NM,
        NULL AS CDL_DTTM,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS CRTD_DTTM,
        NULL AS UPDT_DTTM
    FROM (
            SELECT *
            FROM ITG_MDS_PH_POS_PRICELIST
            WHERE ACTIVE = 'Y'
        ) AS IPP2,
        (
            SELECT SALES.CUST_ITEM_CD,
                'PSC' AS CUST_CD,
                SAP_ITEM_CD,
                CUST_CONV_FACTOR,
                JNJ_PC_PER_CUST_UNIT,
                SALES.MNTH_ID AS JJ_MNTH_ID,
                CASE
                    WHEN SALES.MNTH_ID = IPP.JJ_MNTH_ID THEN IPP.JJ_MNTH_ID
                    WHEN SALES.MNTH_ID != NVL (IPP.JJ_MNTH_ID, '0')
                    AND SALES.EARLY_BK_PERIOD != ''
                    and UPPER(SALES.EARLY_BK_PERIOD) != 'NULL' THEN SALES.EARLY_BK_PERIOD
                    WHEN SALES.MNTH_ID != NVL (IPP.JJ_MNTH_ID, '0')
                    AND (
                        SALES.EARLY_BK_PERIOD IS NULL
                        OR UPPER(SALES.EARLY_BK_PERIOD) = 'NULL'
                    ) THEN SALES.LST_PERIOD
                END AS PL_JJ_MNTH_ID,
                SALES.ITEM_CD AS POS_PROD_CD,
                SALES.STORE_CD,
                SALES.TOT_QTY,
                SALES.TOT_AMT
            FROM 
            (
                SELECT SPM.*,
                    CUST_ITEM_CD,
                    CUST_CD,
                    SAP_ITEM_CD,
                    CUST_CONV_FACTOR,
                    JNJ_PC_PER_CUST_UNIT,
                    LST_PERIOD,
                    EARLY_BK_PERIOD
                FROM (
                        SELECT DISTINCT MNTH_ID,
                            ITEM_CD AS CUST_ITEM_CD,
                            CUST_CD,
                            SAP_ITEM_CD,
                            CUST_CONV_FACTOR,
                            JNJ_PC_PER_CUST_UNIT,
                            LST_PERIOD,
                            EARLY_BK_PERIOD
                        FROM ITG_MDS_PH_POS_PRODUCT
                        WHERE ACTIVE = 'Y'
                            AND UPPER(CUST_CD) = 'PSC'
                    ) IPPPD,
                    prod_dna_load.phlsdl_raw.SDL_PH_POS_711 SPM
                WHERE UPPER(TRIM(IPPPD.CUST_ITEM_CD(+))) = UPPER(TRIM(SPM.ITEM_CD))
                    AND IPPPD.MNTH_ID(+) = SPM.MNTH_ID
            ) as SALES,
                (
                    SELECT *
                    FROM ITG_MDS_PH_POS_PRICELIST
                    WHERE ACTIVE = 'Y'
                ) AS IPP
            WHERE IPP.JJ_MNTH_ID(+) = SALES.MNTH_ID
                AND IPP.ITEM_CD(+) = SALES.SAP_ITEM_CD
        ) AS IPPD
    WHERE UPPER(TRIM(IPP2.ITEM_CD(+))) = UPPER(IPPD.SAP_ITEM_CD)
        AND TRIM(IPP2.JJ_MNTH_ID(+)) = TRIM(IPPD.PL_JJ_MNTH_ID)
),
	dyna AS (
	SELECT CUST_CD,
		IPPD.JJ_MNTH_ID AS JJ_MNTH_ID,
		IPPD.POS_PROD_CD AS ITEM_CD,
		IPPD.CUSTOMER_ID AS BRNCH_CD,
		CAST(IPPD.QTY AS NUMERIC(15, 4)) AS POS_QTY,
		CAST(IPPD.SLS_AMT AS NUMERIC(15, 4)) AS POS_GTS,
		CAST(IPPD.CUST_ITEM_PRC AS NUMERIC(15, 4)) AS POS_ITEM_PRC,
		CAST(IPPD.SLS_AMT AS NUMERIC(15, 4)) * (12.0 / 112.0) AS POS_TAX,
		CAST(IPPD.SLS_AMT AS NUMERIC(15, 4)) * (100.0 / 112.0) AS POS_NTS,
		CAST(IPPD.JNJ_PC_PER_CUST_UNIT AS NUMERIC(15, 4)) AS CONV_FACTOR,
		CAST(IPPD.QTY AS NUMERIC(15, 4)) / CAST(IPPD.JNJ_PC_PER_CUST_UNIT AS NUMERIC(15, 4)) AS JJ_QTY_PC,
		IPP2.LST_PRICE_UNIT AS JJ_ITEM_PRC_PER_PC,
		CAST(IPPD.QTY AS NUMERIC(15, 4)) / CAST(IPPD.JNJ_PC_PER_CUST_UNIT AS NUMERIC(15, 4)) * IPP2.LST_PRICE_UNIT AS JJ_GTS,
		(CAST(IPPD.QTY AS NUMERIC(15, 4)) / CAST(IPPD.JNJ_PC_PER_CUST_UNIT AS NUMERIC(15, 4)) * IPP2.LST_PRICE_UNIT) * (12.0 / 112.0) AS JJ_VAT_AMT,
		(CAST(IPPD.QTY AS NUMERIC(15, 4)) / CAST(IPPD.JNJ_PC_PER_CUST_UNIT AS NUMERIC(15, 4)) * IPP2.LST_PRICE_UNIT) * (100.0 / 112.0) AS JJ_NTS,
		NULL AS FILE_NM,
		NULL AS CDL_DTTM,
		current_timestamp()::timestamp_ntz(9) AS CRTD_DTTM,
		NULL AS UPDT_DTTM
	FROM (
		SELECT *
		FROM ITG_MDS_PH_POS_PRICELIST
		WHERE ACTIVE = 'Y'
		) AS IPP2,
		(
			SELECT SALES.CUST_ITEM_CD,
				'DYNA' AS CUST_CD,
				SAP_ITEM_CD,
				CUST_ITEM_PRC,
				CUST_CONV_FACTOR,
				SALES.MNTH_ID AS JJ_MNTH_ID,
				JNJ_PC_PER_CUST_UNIT,
				CASE 
					WHEN SALES.MNTH_ID = IPP.JJ_MNTH_ID
						THEN IPP.JJ_MNTH_ID
					WHEN SALES.MNTH_ID != NVL(IPP.JJ_MNTH_ID, '0')
						AND SALES.EARLY_BK_PERIOD != ''
						AND UPPER(SALES.EARLY_BK_PERIOD) != 'NULL'
						THEN SALES.EARLY_BK_PERIOD
					WHEN SALES.MNTH_ID != NVL(IPP.JJ_MNTH_ID, '0')
						AND (
							SALES.EARLY_BK_PERIOD IS NULL
							OR UPPER(SALES.EARLY_BK_PERIOD) = 'NULL'
							)
						THEN SALES.LST_PERIOD
					END AS PL_JJ_MNTH_ID,
				SALES.MATL_NUM AS POS_PROD_CD,
				SALES.CUSTOMER_ID,
				SALES.QTY,
				SALES.SLS_AMT
			FROM (
				SELECT SPM.*,
					CUST_ITEM_CD,
					CUST_CD,
					SAP_ITEM_CD,
					CUST_ITEM_PRC,
					CUST_CONV_FACTOR,
					JNJ_PC_PER_CUST_UNIT,
					LST_PERIOD,
					EARLY_BK_PERIOD
				FROM (
					SELECT DISTINCT MNTH_ID,
						ITEM_CD AS CUST_ITEM_CD,
						CUST_CD,
						SAP_ITEM_CD,
						CUST_ITEM_PRC,
						CUST_CONV_FACTOR,
						JNJ_PC_PER_CUST_UNIT,
						LST_PERIOD,
						EARLY_BK_PERIOD
					FROM ITG_MDS_PH_POS_PRODUCT
					WHERE ACTIVE = 'Y'
						AND UPPER(CUST_CD) = 'DYNA'
					) IPPPD,
					SDL_PH_POS_DYNA_SALES SPM
				WHERE UPPER(TRIM(IPPPD.CUST_ITEM_CD(+))) = UPPER(TRIM(SPM.MATL_NUM))
					AND IPPPD.MNTH_ID(+) = SPM.MNTH_ID
				) AS SALES,
				(
					SELECT *
					FROM ITG_MDS_PH_POS_PRICELIST
					WHERE ACTIVE = 'Y'
					) AS IPP
			WHERE IPP.JJ_MNTH_ID(+) = SALES.MNTH_ID
				AND IPP.ITEM_CD(+) = SALES.SAP_ITEM_CD
			) AS IPPD
	WHERE UPPER(TRIM(IPP2.ITEM_CD(+))) = UPPER(IPPD.SAP_ITEM_CD)
		AND TRIM(IPP2.JJ_MNTH_ID(+)) = TRIM(IPPD.PL_JJ_MNTH_ID)
		),
	transformed AS (
            select * from robinsons
            union all
            select * from mercury
            union all
            select * from seveneleven
            union all
            select * from watsons
            union all
            select * from waltermart
            union all
            select * from rustans
            union all
            select * from southstar
            union all
            select * from dyna
		),
	final AS (
		SELECT cust_cd::VARCHAR(30) AS cust_cd,
			jj_mnth_id::VARCHAR(30) AS jj_mnth_id,
			item_cd::VARCHAR(30) AS item_cd,
			brnch_cd::VARCHAR(50) AS brnch_cd,
			pos_qty::number(20, 4) AS pos_qty,
			pos_gts::number(20, 4) AS pos_gts,
			pos_item_prc::number(20, 4) AS pos_item_prc,
			pos_tax::number(20, 4) AS pos_tax,
			pos_nts::number(20, 4) AS pos_nts,
			conv_factor::number(20, 4) AS conv_factor,
			jj_qty_pc::number(20, 4) AS jj_qty_pc,
			jj_item_prc_per_pc::number(20, 4) AS jj_item_prc_per_pc,
			jj_gts::number(20, 4) AS jj_gts,
			jj_vat_amt::number(20, 4) AS jj_vat_amt,
			jj_nts::number(20, 4) AS jj_nts,
			file_nm::VARCHAR(150) AS file_nm,
			cdl_dttm::VARCHAR(50) AS cdl_dttm,
			crtd_dttm::timestamp_ntz(9) AS crtd_dttm,
			updt_dttm::timestamp_ntz(9) AS updt_dttm
		FROM transformed
		)

SELECT * FROM final
