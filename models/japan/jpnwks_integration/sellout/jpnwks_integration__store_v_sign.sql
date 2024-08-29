WITH wk_so_planet_no_dup AS
(
    SELECT * FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
),

mt_trade_conv AS
(
    SELECT * FROM {{ source('jpnedw_integration', 'mt_trade_conv') }}
),

final AS
(
    SELECT 
        a.jcp_rec_seq::NUMBER(10,0) AS jcp_rec_seq,
        coalesce(trim(b.num_sign), '0')::VARCHAR(2) AS v_sign 
    FROM wk_so_planet_no_dup a,
        mt_trade_conv b 
    WHERE b.van_kbn = 'P'
        AND b.sell_inout_kbn = 'O'
        AND b.trade_type_jdnh = a.trade_type
)

SELECT * FROM final