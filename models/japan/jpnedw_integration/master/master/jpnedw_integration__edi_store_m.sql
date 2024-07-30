WITH source AS
(
    SELECT * FROM {{ source('jpnsdl_raw', 'edi_store_m') }}
),

final AS
(
    SELECT
        create_dt::TIMESTAMP_NTZ(9) AS create_dt,
        create_user::VARCHAR(256) AS create_user,
        update_dt::TIMESTAMP_NTZ(9) AS update_dt,
        update_user::VARCHAR(256) AS update_user,
        reg_dt::TIMESTAMP_NTZ(9) AS reg_dt,
        str_cd::VARCHAR(256) AS str_cd,
        lgl_nm_knj1::VARCHAR(256) AS lgl_nm_knj1,
        lgl_nm_knj2::VARCHAR(256) AS lgl_nm_knj2,
        lgl_nm_kn::VARCHAR(256) AS lgl_nm_kn,
        cmmn_nm_knj::VARCHAR(256) AS cmmn_nm_knj,
        cmmn_nm_kn::VARCHAR(256) AS cmmn_nm_kn,
        adrs_knj1::VARCHAR(256) AS adrs_knj1,
        adrs_knj2::VARCHAR(256) AS adrs_knj2,
        adrs_kn::VARCHAR(256) AS adrs_kn,
        pst_co::VARCHAR(256) AS pst_co,
        tel_no::VARCHAR(256) AS tel_no,
        jis_prfct_c::VARCHAR(256) AS jis_prfct_c,
        jis_city_cd::VARCHAR(256) AS jis_city_cd,
        trd_cd::VARCHAR(256) AS trd_cd,
        trd_offc_cd::VARCHAR(256) AS trd_offc_cd,
        chn_cd::VARCHAR(256) AS chn_cd,
        chn_offc_cd::VARCHAR(256) AS chn_offc_cd,
        chn_cd_oth::VARCHAR(256) AS chn_cd_oth,
        emp_cd_kk::VARCHAR(256) AS emp_cd_kk,
        all_str_ass::VARCHAR(256) AS all_str_ass,
        agrm_str::VARCHAR(256) AS agrm_str,
        pj_ass::VARCHAR(256) AS pj_ass,
        emp_cd_roc::VARCHAR(256) AS emp_cd_roc
    FROM source
)

SELECT * FROM final