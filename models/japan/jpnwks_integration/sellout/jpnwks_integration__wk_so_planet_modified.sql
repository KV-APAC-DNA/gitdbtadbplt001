{{
    config
    (
        post_hook = "
                    UPDATE {{this}}
					SET RTL_CD = mds.str_cd
					FROM DEV_DNA_CORE.JPNITG_INTEGRATION.ITG_MDS_JP_MT_SO_RTLR_CHG mds
					INNER JOIN {{this}} tb2 ON mds.rtlr_cd = tb2.rtl_cd
						AND mds.bgn_sndr_cd = tb2.bgn_sndr_cd
						AND mds.rtlr_name = tb2.rtl_name
					INNER JOIN DEV_DNA_CORE.JPNITG_INTEGRATION.DW_SO_PLANET_ERR CE ON tb2.jcp_rec_seq = CE.jcp_rec_seq
					INNER JOIN DEV_DNA_CORE.JPNITG_INTEGRATION.DW_SO_PLANET_ERR_CD_2 cds ON tb2.jcp_rec_seq = cds.jcp_rec_seq
					INNER JOIN DEV_DNA_CORE.JPNWKS_INTEGRATION.CONSISTENCY_ERROR_2 YT ON YT.jcp_rec_seq = tb2.jcp_rec_seq
					WHERE cds.error_cd = 'E004'
						AND CE.export_flag = 0
						AND cds.export_flag = 0
						AND cds.exec_flag = 'AUTOCORRECT';


					UPDATE {{this}}
					SET RTL_CD = chg.str_cd
					FROM DEV_DNA_CORE.JPNEDW_INTEGRATION.EDI_RTLR_CD_CHNG chg
					INNER JOIN {{this}} tb2 ON chg.rtlr_cd = tb2.rtl_cd
						AND chg.bgn_sndr_cd = tb2.bgn_sndr_cd
					INNER JOIN DEV_DNA_CORE.JPNITG_INTEGRATION.DW_SO_PLANET_ERR CE ON tb2.jcp_rec_seq = CE.jcp_rec_seq
					INNER JOIN DEV_DNA_CORE.JPNITG_INTEGRATION.DW_SO_PLANET_ERR_CD_2 cds ON tb2.jcp_rec_seq = cds.jcp_rec_seq
					INNER JOIN DEV_DNA_CORE.JPNWKS_INTEGRATION.CONSISTENCY_ERROR_2 YT ON YT.jcp_rec_seq = tb2.jcp_rec_seq
					WHERE cds.error_cd = 'E004'
						AND CE.export_flag = 0
						AND cds.export_flag = 0
						AND cds.exec_flag = 'AUTOCORRECT';


					UPDATE {{this}}
					SET WS_CD = sv.int_partner_number
					FROM DEV_DNA_CORE.JPNITG_INTEGRATION.ITG_MDS_JP_MT_SO_WS_CHG sv
					INNER JOIN {{this}} tb2 ON sv.ws_cd = tb2.ws_cd
						AND sv.bgn_sndr_cd = tb2.bgn_sndr_cd
					INNER JOIN DEV_DNA_CORE.JPNITG_INTEGRATION.DW_SO_PLANET_ERR CE ON tb2.jcp_rec_seq = CE.jcp_rec_seq
					INNER JOIN DEV_DNA_CORE.JPNITG_INTEGRATION.DW_SO_PLANET_ERR_CD_2 cds ON tb2.jcp_rec_seq = cds.jcp_rec_seq
					INNER JOIN DEV_DNA_CORE.JPNWKS_INTEGRATION.CONSISTENCY_ERROR_2 YT ON YT.jcp_rec_seq = tb2.jcp_rec_seq
					WHERE cds.error_cd = 'E017'
						AND CE.export_flag = 0
						AND cds.export_flag = 0
						AND cds.exec_flag = 'AUTOCORRECT';


					UPDATE {{this}}
					SET PRICE_TYPE = 'K'
					FROM DEV_DNA_CORE.JPNITG_INTEGRATION.DW_SO_PLANET_ERR PR
					INNER JOIN {{this}} tb2 ON tb2.jcp_rec_seq = PR.jcp_rec_seq
						AND PR.bgn_sndr_cd = tb2.bgn_sndr_cd
						AND PR.RTL_CD = tb2.RTL_CD
					INNER JOIN DEV_DNA_CORE.JPNWKS_INTEGRATION.CONSISTENCY_ERROR_2 ERR ON tb2.jcp_rec_seq = ERR.jcp_rec_seq
					INNER JOIN DEV_DNA_CORE.JPNITG_INTEGRATION.DW_SO_PLANET_ERR_CD_2 cds ON tb2.jcp_rec_seq = cds.jcp_rec_seq
					WHERE cds.error_cd = 'E020'
						AND PR.export_flag = 0
						AND cds.export_flag = 0
						AND cds.exec_flag = 'AUTOCORRECT';


					UPDATE {{this}}
					SET unt_prc = CASE 
							WHEN (
									ER.opt_fld = 'S'
									OR ER.opt_fld = 's'
									)
								THEN to_number( ER.unt_prc) / 1.10
							ELSE to_number( ER.unt_prc)
							END,
						net_prc = CASE 
							WHEN (
									ER.opt_fld = 'S'
									OR ER.opt_fld = 's'
									)
								THEN to_number(ER.net_prc) / 1.10
							ELSE to_number( ER.net_prc)
							END
					FROM {{this}} ER;


					UPDATE {{this}}
					SET opt_fld = CASE 
							WHEN (
									ER.opt_fld = 'S'
									OR ER.opt_fld = 's'
									)
								THEN NULL
							ELSE ER.opt_fld
							END
					FROM {{this}} ER;
                "
    )
}}

WITH temp_table_3
AS (
	SELECT *
	FROM {{ ref('jpnwks_integration__temp_table_3') }}
	),
dw_so_planet_err
AS (
	SELECT *
	FROM {{ ref('jpnitg_integration__dw_so_planet_err') }}
	),
consistency_error_2
AS (
	SELECT *
	FROM {{ ref('jpnwks_integration__consistency_error_2') }}
	),
dw_so_planet_err_cd
AS (
	SELECT *
	FROM {{ ref('jpnitg_integration__dw_so_planet_err_cd') }}
	),
dw_so_planet_err_cd_2
AS (
	SELECT *
	FROM {{ ref('jpnitg_integration__dw_so_planet_err_cd_2') }}
	),
temp_table_1
AS (
	SELECT *
	FROM {{ ref('jpnwks_integration__temp_table_1') }}
	),
ct1
AS (
	SELECT DISTINCT a.jcp_rec_seq,
		a.id,
		a.rcv_dt,
		a.test_flag,
		a.bgn_sndr_cd,
		a.ws_cd,
		a.rtl_type,
		a.rtl_cd,
		a.trade_type,
		a.shp_date,
		a.shp_num,
		a.trade_cd,
		a.dep_cd,
		a.chg_cd,
		a.person_in_charge,
		a.person_name,
		a.rtl_name,
		a.rtl_ho_cd,
		a.rtl_address_cd,
		a.data_type,
		a.opt_fld,
		a.item_nm,
		a.item_cd_typ,
		a.item_cd,
		a.qty,
		a.qty_type,
		a.price,
		a.price_type,
		a.bgn_sndr_cd_gln,
		a.rcv_cd_gln,
		a.ws_cd_gln,
		a.shp_ws_cd,
		a.shp_ws_cd_gln,
		a.rep_name_kanji,
		a.rep_info,
		a.trade_cd_gln,
		a.rtl_cd_gln,
		a.rtl_name_kanji,
		a.rtl_ho_cd_gln,
		a.item_cd_gtin,
		a.item_nm_kanji,
		rtrim(ltrim(nullif(a.unt_prc, ''))) AS unt_prc,
		rtrim(ltrim(nullif(a.net_prc, ''))) AS net_prc,
		a.sales_chan_type,
		to_timestamp(substring(current_timestamp(), 1, 19)) AS jcp_create_date
	FROM temp_table_3 a
	WHERE a.jcp_rec_seq NOT IN (
			SELECT DISTINCT prev_rec_seq
			FROM temp_table_3
			WHERE prev_rec_seq IS NOT NULL
				OR jcp_rec_seq IS NULL
			)
	),
ct2
AS (
	SELECT DISTINCT er.jcp_rec_seq,
		er.id,
		er.rcv_dt,
		er.test_flag,
		er.bgn_sndr_cd,
		er.ws_cd,
		er.rtl_type,
		er.rtl_cd,
		er.trade_type,
		er.shp_date,
		er.shp_num,
		er.trade_cd,
		er.dep_cd,
		er.chg_cd,
		er.person_in_charge,
		er.person_name,
		er.rtl_name,
		er.rtl_ho_cd,
		er.rtl_address_cd,
		er.data_type,
		er.opt_fld,
		er.item_nm,
		er.item_cd_typ,
		er.item_cd,
		er.qty,
		er.qty_type,
		er.price,
		er.price_type,
		er.bgn_sndr_cd_gln,
		er.rcv_cd_gln,
		er.ws_cd_gln,
		er.shp_ws_cd,
		er.shp_ws_cd_gln,
		er.rep_name_kanji,
		er.rep_info,
		er.trade_cd_gln,
		er.rtl_cd_gln,
		er.rtl_name_kanji,
		er.rtl_ho_cd_gln,
		er.item_cd_gtin,
		er.item_nm_kanji,
		rtrim(ltrim(nullif(er.unt_prc, ''))) AS unt_prc,
		rtrim(ltrim(nullif(er.net_prc, ''))) AS net_prc,
		er.sales_chan_type,
		to_timestamp(substring(current_timestamp(), 1, 19)) AS jcp_create_date
	FROM dw_so_planet_err er
	INNER JOIN consistency_error_2 r ON r.jcp_rec_seq = er.jcp_rec_seq
	WHERE er.jcp_rec_seq NOT IN (
			SELECT jcp_rec_seq
			FROM dw_so_planet_err_cd_2
			WHERE exec_flag IN ('DELETE', 'MANUAL')
				AND export_flag = 0
			)
		AND er.jcp_rec_seq NOT IN (
			SELECT jcp_rec_seq
			FROM dw_so_planet_err_cd
			WHERE error_cd = 'NRTL'
			)
		AND er.export_flag = 0
		AND er.jcp_rec_seq NOT IN (
			SELECT jcp_rec_seq
			FROM temp_table_1
			)
	),
trns
AS (
	SELECT *
	FROM ct1
	
	UNION ALL
	
	SELECT *
	FROM ct2
	),
final
AS (
	SELECT jcp_rec_seq::number(10, 0) AS jcp_rec_seq,
		id::number(10, 0) AS id,
		rcv_dt::VARCHAR(256) AS rcv_dt,
		test_flag::VARCHAR(256) AS test_flag,
		bgn_sndr_cd::VARCHAR(256) AS bgn_sndr_cd,
		ws_cd::VARCHAR(256) AS ws_cd,
		rtl_type::VARCHAR(256) AS rtl_type,
		rtl_cd::VARCHAR(256) AS rtl_cd,
		trade_type::VARCHAR(256) AS trade_type,
		shp_date::VARCHAR(256) AS shp_date,
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
		qty::VARCHAR(256) AS qty,
		qty_type::VARCHAR(256) AS qty_type,
		price::VARCHAR(256) AS price,
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
		unt_prc::VARCHAR(256) AS unt_prc,
		net_prc::VARCHAR(256) AS net_prc,
		sales_chan_type::VARCHAR(256) AS sales_chan_type,
		jcp_create_date::timestamp_ntz(9) AS jcp_create_date
	FROM trns
	)
SELECT *
FROM final