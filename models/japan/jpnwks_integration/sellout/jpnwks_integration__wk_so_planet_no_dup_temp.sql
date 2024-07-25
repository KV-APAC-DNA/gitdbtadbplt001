WITH source AS
(
    SELECT * FROM {{ ref('jpnwks_integration__wk_so_planet_revise') }}
),

final AS
(
    SELECT 
        r.jcp_rec_seq::NUMBER(10,0) as jcp_rec_seq,
        r.id::NUMBER(10,0) as id,
        r.rcv_dt::VARCHAR(256) as rcv_dt,
        r.test_flag::VARCHAR(256) as test_flag,
        r.bgn_sndr_cd::VARCHAR(256) as bgn_sndr_cd,
        r.ws_cd::VARCHAR(256) as ws_cd,
        r.rtl_type::VARCHAR(256) as rtl_type,
        r.rtl_cd::VARCHAR(256) as rtl_cd,
        r.trade_type::VARCHAR(256) as trade_type,
        r.shp_date::VARCHAR(256) as shp_date,
        r.shp_num::VARCHAR(256) as shp_num,
        r.trade_cd::VARCHAR(256) as trade_cd,
        r.dep_cd::VARCHAR(256) as dep_cd,
        r.chg_cd::VARCHAR(256) as chg_cd,
        r.person_in_charge::VARCHAR(256) as person_in_charge,
        r.person_name::VARCHAR(256) as person_name,
        null::VARCHAR(256) as column_17,
        r.rtl_name::VARCHAR(256) as rtl_name,
        r.rtl_ho_cd::VARCHAR(256) as rtl_ho_cd,
        r.rtl_address_cd::VARCHAR(256) as rtl_address_cd_01,
        null::VARCHAR(256) as rtl_address_cd_02,
        r.data_type::VARCHAR(256) as data_type,
        r.opt_fld::VARCHAR(256) as opt_fld,
        r.item_nm::VARCHAR(256) as item_nm,
        r.item_cd_typ::VARCHAR(256) as item_cd_typ,
        r.item_cd::VARCHAR(256) as item_cd,
        r.qty::VARCHAR(256) as qty,
        r.qty_type::VARCHAR(256) as qty_type,
        r.price::VARCHAR(256) as price,
        r.price_type::VARCHAR(256) as price_type,
        r.bgn_sndr_cd_gln::VARCHAR(256) as bgn_sndr_cd_gln,
        r.rcv_cd_gln::VARCHAR(256) as rcv_cd_gln,
        r.ws_cd_gln::VARCHAR(256) as ws_cd_gln,
        r.shp_ws_cd::VARCHAR(256) as shp_ws_cd,
        r.shp_ws_cd_gln::VARCHAR(256) as shp_ws_cd_gln,
        r.rep_name_kanji::VARCHAR(256) as rep_name_kanji,
        r.rep_info::VARCHAR(256) as rep_info,
        r.trade_cd_gln::VARCHAR(256) as trade_cd_gln,
        r.rtl_cd_gln::VARCHAR(256) as rtl_cd_gln,
        r.rtl_name_kanji::VARCHAR(256) as rtl_name_kanji,
        r.rtl_ho_cd_gln::VARCHAR(256) as rtl_ho_cd_gln,
        r.item_cd_gtin::VARCHAR(256) as item_cd_gtin,
        r.item_nm_kanji::VARCHAR(256) as item_nm_kanji,
        r.unt_prc::VARCHAR(256) as unt_prc,
        r.net_prc::VARCHAR(256) as net_prc,
        r.sales_chan_type::VARCHAR(256) as sales_chan_type,
        r.jcp_create_date::TIMESTAMP_NTZ(9) as jcp_create_date
    FROM source r
)

SELECT * FROM final