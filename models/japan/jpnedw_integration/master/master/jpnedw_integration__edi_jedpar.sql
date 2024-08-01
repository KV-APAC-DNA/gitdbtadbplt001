WITH source AS
(
    SELECT * FROM {{ source('jpnsdl_raw', 'edi_jedpar') }}
),

final AS
(
    SELECT
        create_dt::TIMESTAMP_NTZ(9) AS create_dt,
        create_user::VARCHAR(256) AS create_user,
        update_dt::TIMESTAMP_NTZ(9) AS update_dt,
        update_user::VARCHAR(256) AS update_user,
        customer_number::VARCHAR(256) AS customer_number,
        partner_fn::VARCHAR(256) AS partner_fn,
        int_partner_number::VARCHAR(256) AS int_partner_number,
        sap_cstm_type::VARCHAR(256) AS sap_cstm_type,
        status::VARCHAR(256) AS status,
        partner_customer_cd::VARCHAR(256) AS partner_customer_cd,
        ext_partner_number::VARCHAR(256) AS ext_partner_number,
        van_type::VARCHAR(256) AS van_type
    FROM source
)

SELECT * FROM final