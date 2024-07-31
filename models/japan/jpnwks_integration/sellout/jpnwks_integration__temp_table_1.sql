WITH dw_so_planet_err
AS (
    SELECT *
    FROM {{ ref('jpnitg_integration__dw_so_planet_err') }}
    ),
dw_so_planet_err_cd_2
AS (
    SELECT *
    FROM {{ ref('jpnitg_integration__dw_so_planet_err_cd_2') }}
    ),
dw_so_planet_err_cd
AS (
    SELECT *
    FROM {{ ref('jpnitg_integration__dw_so_planet_err_cd') }}
    ),
consistency_error_2
AS (
    SELECT *
    FROM {{ ref('jpnwks_integration__consistency_error_2') }}
    ),
itg_mds_jp_mt_so_item_chg
AS (
    SELECT *
    FROM {{ ref('jpnitg_integration__itg_mds_jp_mt_so_item_chg') }}
    ),
trns
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
        CASE 
            WHEN mdsitem.error_type IN (1, 2)
                AND er.item_cd = mdsitem.ext_jan_cd
                AND er.bgn_sndr_cd = cast(mdsitem.bgn_sndr_cd AS VARCHAR)
                THEN mdsitem.int_jan_cd
            ELSE er.item_cd
            END AS item_cd,
        CASE 
            WHEN mdsitem.error_type IN (1, 2)
                AND er.item_cd = mdsitem.ext_jan_cd
                AND er.bgn_sndr_cd = cast(mdsitem.bgn_sndr_cd AS VARCHAR)
                THEN cast((mdsitem.qty * er.qty) AS VARCHAR)
            ELSE er.qty
            END AS qty,
        er.qty_type,
        er.price,
        CASE 
            WHEN mdsitem.error_type = 2
                AND er.item_cd = mdsitem.ext_jan_cd
                AND er.bgn_sndr_cd = cast(mdsitem.bgn_sndr_cd AS VARCHAR)
                THEN 'T'
            WHEN mdsitem.error_type = 1
                AND er.item_cd = mdsitem.ext_jan_cd
                AND er.bgn_sndr_cd = cast(mdsitem.bgn_sndr_cd AS VARCHAR)
                THEN 'K'
            ELSE er.price_type
            END AS price_type,
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
        CASE 
            WHEN mdsitem.error_type = 1
                AND er.item_cd = mdsitem.ext_jan_cd
                AND er.bgn_sndr_cd = mdsitem.bgn_sndr_cd
                THEN cast(er.unt_prc AS VARCHAR)
            WHEN mdsitem.error_type = 2
                AND er.item_cd = mdsitem.ext_jan_cd
                AND er.bgn_sndr_cd = mdsitem.bgn_sndr_cd
                THEN cast(mdsitem.unit_price AS VARCHAR)
            ELSE er.unt_prc
            END AS unt_prc,
        CASE 
            WHEN mdsitem.error_type = 1
                AND er.item_cd = mdsitem.ext_jan_cd
                AND er.bgn_sndr_cd = mdsitem.bgn_sndr_cd
                THEN cast(er.net_prc AS VARCHAR)
            WHEN mdsitem.error_type = 2
                AND er.item_cd = mdsitem.ext_jan_cd
                AND er.bgn_sndr_cd = mdsitem.bgn_sndr_cd
                THEN cast((er.qty::DECIMAL * er.unt_prc::DECIMAL * mdsitem.qty::DECIMAL) AS VARCHAR)
            ELSE er.net_prc
            END AS net_prc,
        er.sales_chan_type,
        to_timestamp(left(current_timestamp::TEXT, 19)) AS jcp_create_date
    FROM dw_so_planet_err er
    INNER JOIN consistency_error_2 r ON r.jcp_rec_seq = er.jcp_rec_seq
        AND er.export_flag = 0
        AND r.error_cd = 'ERR_012'
        AND r.jcp_rec_seq NOT IN (
            SELECT jcp_rec_seq
            FROM dw_so_planet_err_cd_2
            WHERE exec_flag IN ('DELETE', 'MANUAL')
                AND export_flag = 0
            )
    LEFT JOIN itg_mds_jp_mt_so_item_chg mdsitem ON er.bgn_sndr_cd = mdsitem.bgn_sndr_cd
        AND er.item_cd = mdsitem.ext_jan_cd
        AND mdsitem.error_type IN (1, 2)
    WHERE er.jcp_rec_seq NOT IN (
            SELECT jcp_rec_seq
            FROM dw_so_planet_err_cd
            WHERE error_cd = 'NRTL'
            )
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