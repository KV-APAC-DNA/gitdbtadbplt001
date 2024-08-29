WITH source AS
(
    SELECT * FROM {{ source('jpnsdl_raw', 'edi_cstm_m') }}
),

final AS
(
    SELECT
        create_dt::TIMESTAMP_NTZ(9) AS create_dt,
        create_user::VARCHAR(256) AS create_user,
        update_dt::TIMESTAMP_NTZ(9) AS update_dt,
        update_user::VARCHAR(256) AS update_user,
        reg_dt::TIMESTAMP_NTZ(9) AS reg_dt,
        cstm_cd::VARCHAR(256) AS cstm_cd,
        cstm_nm::VARCHAR(256) AS cstm_nm,
        cstm_nm_kn::VARCHAR(256) AS cstm_nm_kn,
        cstm_nm_knj::VARCHAR(256) AS cstm_nm_knj,
        adrs::VARCHAR(256) AS adrs,
        adrs_kn::VARCHAR(256) AS adrs_kn,
        adrs_knj::VARCHAR(256) AS adrs_knj,
        pst_cd::VARCHAR(256) AS pst_cd,
        tel_num::VARCHAR(256) AS tel_num,
        fax_nun::VARCHAR(256) AS fax_nun,
        plnt_cd::VARCHAR(256) AS plnt_cd,
        ship_dpt::VARCHAR(256) AS ship_dpt,
        ship_ld_tm::NUMBER(5,2) AS ship_ld_tm,
        jis_prfct_cd::VARCHAR(256) AS jis_prfct_cd,
        jis_city_cd::VARCHAR(256) AS jis_city_cd,
        cstm_typ::VARCHAR(256) AS cstm_typ,
        bl_cls_dt::VARCHAR(256) AS bl_cls_dt,
        trd_typ_cg_flg::VARCHAR(256) AS trd_typ_cg_flg,
        jrsd_dpt_cd::VARCHAR(256) AS jrsd_dpt_cd,
        acnt_prsn_cd::VARCHAR(256) AS acnt_prsn_cd,
        buy_from_cd::VARCHAR(256) AS buy_from_cd,
        rebate_rep_cd::VARCHAR(256) AS rebate_rep_cd,
        updateflg::VARCHAR(256) AS updateflg,
        cust_tfi_num::VARCHAR(256) AS cust_tfi_num,
        transporter_id::VARCHAR(256) AS transporter_id,
        transport_fee_id::VARCHAR(256) AS transport_fee_id,
        transport_timing::VARCHAR(256) AS transport_timing,
        cmnt1::VARCHAR(256) AS cmnt1
    FROM source
)

SELECT * FROM final