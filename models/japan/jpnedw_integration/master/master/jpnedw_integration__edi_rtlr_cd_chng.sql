WITH source AS
(
    SELECT * FROM {{ source('jpnsdl_raw', 'edi_rtlr_cd_chng') }}
),

final AS
(
    SELECT
        create_dt::TIMESTAMP_NTZ(9) AS create_dt,
        create_user::VARCHAR(256) AS create_user,
        update_dt::TIMESTAMP_NTZ(9) AS update_dt,
        update_user::VARCHAR(256) AS update_user,
        reg_dt::TIMESTAMP_NTZ(9) AS reg_dt,
        bgn_sndr_cd::VARCHAR(256) AS bgn_sndr_cd,
        rtlr_cd::VARCHAR(256) AS rtlr_cd,
        str_cd::VARCHAR(256) AS str_cd
    FROM source
)

SELECT * FROM final