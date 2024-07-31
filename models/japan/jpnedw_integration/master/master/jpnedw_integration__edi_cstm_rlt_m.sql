WITH source AS
(
    SELECT * FROM {{ source('jpnsdl_raw', 'edi_cstm_rlt_m') }}
),

final AS
(
    SELECT
        create_dt::TIMESTAMP_NTZ(9) AS create_dt,
        create_user::VARCHAR(256) AS create_user,
        update_dt::TIMESTAMP_NTZ(9) AS update_dt,
        update_user::VARCHAR(256) AS update_user,
        sold_to_cstm::VARCHAR(256) AS sold_to_cstm,
        ship_to_cstm::VARCHAR(256) AS ship_to_cstm,
        bill_to_cstm::VARCHAR(256) AS bill_to_cstm,
	    pay_cstm::VARCHAR(256) AS pay_cstm
    FROM source
)

SELECT * FROM final