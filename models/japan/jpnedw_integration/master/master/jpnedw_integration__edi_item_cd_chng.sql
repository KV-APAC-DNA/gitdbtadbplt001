WITH source AS
(
    SELECT * FROM {{ source('jpnsdl_raw', 'edi_item_cd_chng') }}
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
        item_cd_typ::VARCHAR(256) AS item_cd_typ,
        whls_item_cd::VARCHAR(256) AS whls_item_cd,
        cpny_item_cd::VARCHAR(256) AS cpny_item_cd,
        pc::NUMBER(30,0) AS pc
    FROM source
)

SELECT * FROM final