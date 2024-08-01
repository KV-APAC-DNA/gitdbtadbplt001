WITH source AS
(
    SELECT * FROM {{ source('jpnsdl_raw', 'edi_chn_m') }}
),

final AS
(
    SELECT
        create_dt::TIMESTAMP_NTZ(9) AS create_dt,
        create_user::VARCHAR(256) AS create_user,
        update_dt::TIMESTAMP_NTZ(9) AS update_dt,
        update_user::VARCHAR(256) AS update_user,
        reg_dt::TIMESTAMP_NTZ(9) AS reg_dt,
        chn_cd::VARCHAR(256) AS chn_cd,
        lgl_nm::VARCHAR(256) AS lgl_nm,
        cmmn_nm::VARCHAR(256) AS cmmn_nm,
        adrs::VARCHAR(256) AS adrs,
        acnt_prsn_cd::VARCHAR(256) AS acnt_prsn_cd,
        rank::VARCHAR(256) AS rank,
        chn_offc_cd::VARCHAR(256) AS chn_offc_cd,
        frnc::VARCHAR(256) AS frnc,
        sgmt::VARCHAR(256) AS sgmt,
        an_typ::VARCHAR(256) AS an_typ,
        pj_typ::VARCHAR(256) AS pj_typ,
        sales_group::VARCHAR(256) AS sales_group,
        scnd_acnt_prsn::VARCHAR(8) AS scnd_acnt_prsn
    FROM source
)

SELECT * FROM final