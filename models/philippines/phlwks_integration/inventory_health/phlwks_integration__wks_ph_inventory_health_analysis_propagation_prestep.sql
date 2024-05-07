with
wks_ph_regional as
(
    select * from {{ ref('phlwks_integration__wks_ph_regional') }}
),
trans as
(
    SELECT *
FROM (
	WITH Regional AS (
			SELECT *,
				SUM(SI_GTS_VAL) OVER (
					PARTITION BY country_name,
					year,
					MNTH_ID
					) AS SI_INV_DB_VAL,
				SUM(SI_GTS_VAL_USD) OVER (
					PARTITION BY country_name,
					year,
					MNTH_ID
					) AS SI_INV_DB_VAL_USD
			FROM wks_ph_regional
			WHERE (country_name || SAP_PRNT_CUST_DESC) IN (
					SELECT country_name || SAP_PRNT_CUST_DESC
					FROM (
						SELECT country_name,
							SAP_PRNT_CUST_DESC,
							NVL(SUM(INVENTORY_VAL), 0) AS INV_VAL,
							NVL(SUM(SO_GRS_TRD_SLS), 0) AS Sellout_val
						FROM wks_ph_regional
						WHERE SAP_PRNT_CUST_DESC IS NOT NULL
						GROUP BY country_name,
							SAP_PRNT_CUST_DESC
						HAVING INV_VAL <> 0
						)
					)
			)
	SELECT year,
		qrtr_no,
		mnth_id,
		mnth_no,
		country_name,
		dstrbtr_grp_cd,
		dstrbtr_grp_cd_nm,
		global_prod_franchise,
		global_prod_brand,
		global_prod_sub_brand,
		global_prod_variant,
		global_prod_segment,
		global_prod_subsegment,
		global_prod_category,
		global_prod_subcategory,
		pka_size_desc AS global_put_up_desc,
		sku_cd,
		sku_description,
		pka_product_key,
		pka_product_key_description,
		product_key,
		product_key_description,
		from_ccy,
		to_ccy,
		exch_rate,
		sap_prnt_cust_key,
		sap_prnt_cust_desc,
		sap_cust_chnl_key,
		sap_cust_chnl_desc,
		sap_cust_sub_chnl_key,
		sap_sub_chnl_desc,
		sap_go_to_mdl_key,
		sap_go_to_mdl_desc,
		sap_bnr_key,
		sap_bnr_desc,
		sap_bnr_frmt_key,
		sap_bnr_frmt_desc,
		retail_env,
		region,
		zone_or_area,
		round(cast(si_sls_qty AS NUMERIC(38, 5)), 5) AS si_sls_qty,
		round(cast(si_gts_val AS NUMERIC(38, 5)), 5) AS si_gts_val,
		round(cast(si_gts_val_usd AS NUMERIC(38, 5)), 5) AS si_gts_val_usd,
		round(cast(inventory_quantity AS NUMERIC(38, 5)), 5) AS inventory_quantity,
		round(cast(inventory_val AS NUMERIC(38, 5)), 5) AS inventory_val,
		round(cast(inventory_val_usd AS NUMERIC(38, 5)), 5) AS inventory_val_usd,
		round(cast(so_sls_qty AS NUMERIC(38, 5)), 5) AS so_sls_qty,
		round(cast(so_grs_trd_sls AS NUMERIC(38, 5)), 5) AS so_trd_sls,
		so_grs_trd_sls_usd AS so_trd_sls_usd,
		last_3months_so_qty,
		last_6months_so_qty,
		last_12months_so_qty,
		last_3months_so_val,
		last_3months_so_val_usd,
		last_6months_so_val,
		last_6months_so_val_usd,
		last_12months_so_val,
		last_12months_so_val_usd,
		propagate_flag,
		propagate_from,
		reason,
		last_36months_so_val
	FROM Regional
)
),
final as
(
    select 
    year::integer as year,
    qrtr_no::varchar(14) as qrtr_no,
    mnth_id::varchar(21) as mnth_id,
    mnth_no::integer as mnth_no,
    country_name::varchar(11) as country_name,
    dstrbtr_grp_cd::varchar(50) as dstrbtr_grp_cd,
    dstrbtr_grp_cd_nm::varchar(308) as dstrbtr_grp_cd_nm,
    global_prod_franchise::varchar(30) as global_prod_franchise,
    global_prod_brand::varchar(30) as global_prod_brand,
    global_prod_sub_brand::varchar(100) as global_prod_sub_brand,
    global_prod_variant::varchar(100) as global_prod_variant,
    global_prod_segment::varchar(50) as global_prod_segment,
    global_prod_subsegment::varchar(100) as global_prod_subsegment,
    global_prod_category::varchar(50) as global_prod_category,
    global_prod_subcategory::varchar(50) as global_prod_subcategory,
    global_put_up_desc::varchar(30) as global_put_up_desc,
    sku_cd::varchar(255) as sku_cd,
    sku_description::varchar(100) as sku_description,
    pka_product_key::varchar(68) as pka_product_key,
    pka_product_key_description::varchar(255) as pka_product_key_description,
    product_key::varchar(68) as product_key,
    product_key_description::varchar(255) as product_key_description,
    from_ccy::varchar(5) as from_ccy,
    to_ccy::varchar(5) as to_ccy,
    exch_rate::number(15,5) as exch_rate,
    sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
    sap_prnt_cust_desc::varchar(50) as sap_prnt_cust_desc,
    sap_cust_chnl_key::varchar(12) as sap_cust_chnl_key,
    sap_cust_chnl_desc::varchar(50) as sap_cust_chnl_desc,
    sap_cust_sub_chnl_key::varchar(12) as sap_cust_sub_chnl_key,
    sap_sub_chnl_desc::varchar(50) as sap_sub_chnl_desc,
    sap_go_to_mdl_key::varchar(12) as sap_go_to_mdl_key,
    sap_go_to_mdl_desc::varchar(50) as sap_go_to_mdl_desc,
    sap_bnr_key::varchar(12) as sap_bnr_key,
    sap_bnr_desc::varchar(50) as sap_bnr_desc,
    sap_bnr_frmt_key::varchar(12) as sap_bnr_frmt_key,
    sap_bnr_frmt_desc::varchar(50) as sap_bnr_frmt_desc,
    retail_env::varchar(50) as retail_env,
    region::varchar(255) as region,
    zone_or_area::varchar(255) as zone_or_area,
    si_sls_qty::number(38,5) as si_sls_qty,
    si_gts_val::number(38,5) as si_gts_val,
    si_gts_val_usd::number(38,5) as si_gts_val_usd,
    inventory_quantity::number(38,5) as inventory_quantity,
    inventory_val::number(38,5) as inventory_val,
    inventory_val_usd::number(38,5) as inventory_val_usd,
    so_sls_qty::number(38,5) as so_sls_qty,
    so_trd_sls::number(38,5) as so_trd_sls,
    so_trd_sls_usd::number(22,0) as so_trd_sls_usd,
    last_3months_so_qty::number(38,6) as last_3months_so_qty,
    last_6months_so_qty::number(38,6) as last_6months_so_qty,
    last_12months_so_qty::number(38,6) as last_12months_so_qty,
    last_3months_so_val::number(38,12) as last_3months_so_val,
    last_3months_so_val_usd::number(38,5) as last_3months_so_val_usd,
    last_6months_so_val::number(38,12) as last_6months_so_val,
    last_6months_so_val_usd::number(38,5) as last_6months_so_val_usd,
    last_12months_so_val::number(38,12) as last_12months_so_val,
    last_12months_so_val_usd::number(38,5) as last_12months_so_val_usd,
    propagate_flag::varchar(1) as propagate_flag,
    propagate_from::integer as propagate_from,
    reason::varchar(100) as reason,
    last_36months_so_val::number(38,12) as last_36months_so_val
    from trans
)
select * from final