{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'append',
    )
}}


WITH wk_so_planet_cleansed
AS (
	SELECT *
	FROM {{ ref('jpnwks_integration__wk_so_planet_cleansed') }}
	),
trns
AS (
	SELECT DISTINCT jcp_rec_seq,
		id,
		rcv_dt,
		test_flag,
		bgn_sndr_cd,
		ws_cd,
		rtl_type,
		rtl_cd,
		trade_type,
		shp_date,
		shp_num,
		trade_cd,
		dep_cd,
		chg_cd,
		person_in_charge,
		person_name,
		rtl_name,
		rtl_ho_cd,
		rtl_address_cd,
		data_type,
		opt_fld,
		item_nm,
		item_cd_typ,
		item_cd,
		nullif(qty, '')::INTEGER AS qty,
		qty_type,
		nullif(price, '')::INTEGER AS price,
		price_type,
		bgn_sndr_cd_gln,
		rcv_cd_gln,
		ws_cd_gln,
		shp_ws_cd,
		shp_ws_cd_gln,
		rep_name_kanji,
		rep_info,
		trade_cd_gln,
		rtl_cd_gln,
		rtl_name_kanji,
		rtl_ho_cd_gln,
		item_cd_gtin,
		item_nm_kanji,
		abs(unt_prc) as unt_prc,
		net_prc,
		sales_chan_type,
		jcp_create_date,
		jcp_add_shp_to_cd,
		jcp_add_str_cd,
		jcp_net_price
	FROM wk_so_planet_cleansed
	WHERE to_char(shp_date, 'yyyymm') <= (
			SELECT to_char(add_months(max(rcv_dt), 6), 'yyyymm')
			FROM wk_so_planet_cleansed
			)
	),
final
AS (
	SELECT jcp_rec_seq::number(10, 0) AS jcp_rec_seq,
		id::number(10, 0) AS id,
		rcv_dt::timestamp_ntz(9) AS rcv_dt,
		test_flag::VARCHAR(256) AS test_flag,
		bgn_sndr_cd::VARCHAR(256) AS bgn_sndr_cd,
		ws_cd::VARCHAR(256) AS ws_cd,
		rtl_type::VARCHAR(256) AS rtl_type,
		rtl_cd::VARCHAR(256) AS rtl_cd,
		trade_type::VARCHAR(256) AS trade_type,
		shp_date::timestamp_ntz(9) AS shp_date,
		shp_num::VARCHAR(256) AS shp_num,
		trade_cd::VARCHAR(256) AS trade_cd,
		dep_cd::VARCHAR(256) AS dep_cd,
		chg_cd::VARCHAR(256) AS chg_cd,
		person_in_charge::VARCHAR(256) AS person_in_charge,
		person_name::VARCHAR(256) AS person_name,
		rtl_name::VARCHAR(256) AS rtl_name,
		rtl_ho_cd::VARCHAR(256) AS rtl_ho_cd,
		rtl_address_cd::VARCHAR(256) AS rtl_address_cd,
		data_type::VARCHAR(256) AS data_type,
		opt_fld::VARCHAR(256) AS opt_fld,
		item_nm::VARCHAR(256) AS item_nm,
		item_cd_typ::VARCHAR(256) AS item_cd_typ,
		item_cd::VARCHAR(256) AS item_cd,
		qty::number(30, 0) AS qty,
		qty_type::VARCHAR(256) AS qty_type,
		price::number(30, 0) AS price,
		price_type::VARCHAR(256) AS price_type,
		bgn_sndr_cd_gln::VARCHAR(256) AS bgn_sndr_cd_gln,
		rcv_cd_gln::VARCHAR(256) AS rcv_cd_gln,
		ws_cd_gln::VARCHAR(256) AS ws_cd_gln,
		shp_ws_cd::VARCHAR(256) AS shp_ws_cd,
		shp_ws_cd_gln::VARCHAR(256) AS shp_ws_cd_gln,
		rep_name_kanji::VARCHAR(256) AS rep_name_kanji,
		rep_info::VARCHAR(256) AS rep_info,
		trade_cd_gln::VARCHAR(256) AS trade_cd_gln,
		rtl_cd_gln::VARCHAR(256) AS rtl_cd_gln,
		rtl_name_kanji::VARCHAR(256) AS rtl_name_kanji,
		rtl_ho_cd_gln::VARCHAR(256) AS rtl_ho_cd_gln,
		item_cd_gtin::VARCHAR(256) AS item_cd_gtin,
		item_nm_kanji::VARCHAR(256) AS item_nm_kanji,
		unt_prc::number(30, 0) AS unt_prc,
		net_prc::number(30, 0) AS net_prc,
		sales_chan_type::VARCHAR(256) AS sales_chan_type,
		jcp_create_date::timestamp_ntz(9) AS jcp_create_date,
		jcp_add_shp_to_cd::VARCHAR(10) AS jcp_shp_to_cd,
		jcp_add_str_cd::VARCHAR(9) AS jcp_str_cd,
		jcp_net_price::number(30, 0) AS jcp_net_price
	FROM trns
	)
SELECT *
FROM final