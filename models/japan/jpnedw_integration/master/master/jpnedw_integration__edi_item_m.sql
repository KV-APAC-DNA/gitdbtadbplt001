WITH source AS
(
    SELECT * FROM {{ source('jpnsdl_raw', 'edi_item_m') }}
),

final AS
(
    SELECT
        create_dt::TIMESTAMP_NTZ(9) AS create_dt,
        create_user::VARCHAR(256) AS create_user,
        update_dt::TIMESTAMP_NTZ(9) AS update_dt,
        update_user::VARCHAR(256) AS update_user,
        reg_dt::TIMESTAMP_NTZ(9) AS reg_dt,
        item_cd::VARCHAR(256) AS item_cd,
        item_nm::VARCHAR(256) AS item_nm,
        iten_nm_kn::VARCHAR(256) AS iten_nm_kn,
        iten_nm_knj::VARCHAR(256) AS iten_nm_knj,
        jan_cd::VARCHAR(256) AS jan_cd,
        itf_cd::VARCHAR(256) AS itf_cd,
        pc::VARCHAR(256) AS pc,
        unt_prc::VARCHAR(6) AS unt_prc,
        sub_frnch::VARCHAR(256) AS sub_frnch,
        jan_cd_so::VARCHAR(256) AS jan_cd_so,
        itf_cd_so::VARCHAR(256) AS itf_cd_so,
        updateflg::VARCHAR(1) AS updateflg,
        base_prod::VARCHAR(256) AS base_prod,
        variant::VARCHAR(256) AS variant,
        put_up::VARCHAR(256) AS put_up,
        mega_brnd::VARCHAR(256) AS mega_brnd,
        brnd::VARCHAR(256) AS brnd,
        dlt_flg::VARCHAR(256) AS dlt_flg,
        base_uom::VARCHAR(256) AS base_uom,
        item_cd_jd::VARCHAR(256) AS item_cd_jd,
        sap_cstm_type::VARCHAR(256) AS sap_cstm_type,
        mega_brnd_chkflg::VARCHAR(256) AS mega_brnd_chkflg,
        planet_l3_flg::VARCHAR(256) AS planet_l3_flg,
        rel_dt::TIMESTAMP_NTZ(9) AS rel_dt,
        discon_dt::TIMESTAMP_NTZ(9) AS discon_dt,
        new_prod_type::VARCHAR(256) AS new_prod_type,
        prom_goods_flg::VARCHAR(256) AS prom_goods_flg,
        parent_item_cd::VARCHAR(256) AS parent_item_cd,
        imp_item_flg::VARCHAR(256) AS imp_item_flg,
        succeeding_item_cd::VARCHAR(256) AS succeeding_item_cd,
        ldw_flg01::VARCHAR(256) AS ldw_flg01,
        ldw_flg02::VARCHAR(256) AS ldw_flg02,
        ldw_flg03::VARCHAR(256) AS ldw_flg03
    FROM source
)

SELECT * FROM final