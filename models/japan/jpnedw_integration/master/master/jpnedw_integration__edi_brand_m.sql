WITH source AS
(
    SELECT * FROM {{ source('jpnsdl_raw', 'edi_brand_m') }}
),

final AS
(
    SELECT
        create_dt::TIMESTAMP_NTZ(9) AS create_dt,
        create_user::VARCHAR(256) AS create_user,
        update_dt::TIMESTAMP_NTZ(9) AS update_dt,
        update_user::VARCHAR(256) AS update_user,
        mega_brand::VARCHAR(256) AS mega_brand,
        sap_cstm_type::VARCHAR(256) AS sap_cstm_type
    FROM source
)

SELECT * FROM final