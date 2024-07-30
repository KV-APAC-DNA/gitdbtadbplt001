{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['ph_cd']
    )
}}

WITH source AS
(
    SELECT * FROM {{ source('jpnsdl_raw', 'edi_frnch_m') }}
),

final AS
(
    SELECT
        create_dt::TIMESTAMP_NTZ(9) AS create_dt,
        create_user::VARCHAR(256) AS create_user,
        update_dt::TIMESTAMP_NTZ(9) AS update_dt,
        update_user::VARCHAR(256) AS update_user,
        ph_cd::VARCHAR(256) AS ph_cd,
        ph_lvl::VARCHAR(256) AS ph_lvl,
        ph_nm::VARCHAR(256) AS ph_nm
    FROM source
)

SELECT * FROM final