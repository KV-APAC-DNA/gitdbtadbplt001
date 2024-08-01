WITH source AS
(
    SELECT * FROM {{ source('jpnsdl_raw', 'edi_bgn_sndr') }}
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
        dt_typ::VARCHAR(256) AS dt_typ,
        nm::VARCHAR(256) AS nm,
        lmp_dlt::VARCHAR(256) AS lmp_dlt,
        tax_include::VARCHAR(256) AS tax_include,
        tax_round::VARCHAR(256) AS tax_round,
        van_type::VARCHAR(256) AS van_type
    FROM source
)

SELECT * FROM final