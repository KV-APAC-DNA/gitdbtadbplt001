with source as
(
    select * from {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}
),

final as
(
    SELECT  
        kokyano::VARCHAR(68) AS kokyano,
        saleno::VARCHAR(63) AS saleno,
        gyono::number(18, 0) AS gyono,
        bun_gyono::number(18, 0) AS bun_gyono,
        order_dt::DATE AS order_dt,
        ship_dt::DATE AS ship_dt,
        storename::VARCHAR(131) AS storename,
        f_order::VARCHAR(5) AS f_order,
        f_ship445::VARCHAR(5) AS f_ship445,
        channel::VARCHAR(20) AS channel,
        dspromcode::number(18, 0) AS dspromcode,
        dspromname::VARCHAR(384) AS dspromname,
        juchkbn::VARCHAR(3) AS juchkbn,
        h_o_item_code::VARCHAR(30) AS h_o_item_code,
        h_o_item_name::VARCHAR(200) AS h_o_item_name,
        h_o_item_anbun_qty::number(16, 8) AS h_o_item_anbun_qty,
        h_item_code::VARCHAR(60) AS h_item_code,
        z_item_code::VARCHAR(45) AS z_item_code,
        anbun_soryo::FLOAT AS anbun_soryo,
        gts::FLOAT AS gts,
        gts_qty::FLOAT AS gts_qty,
        ciw_discount::FLOAT AS ciw_discount,
        ciw_point::FLOAT AS ciw_point,
        ciw_return::FLOAT AS ciw_return,
        ciw_return_qty::FLOAT AS ciw_return_qty,
        nts::FLOAT AS nts,
        teikikeiyaku::VARCHAR(20) AS teikikeiyaku,
        f::number(38, 18) AS f,
        z_o_item_code::VARCHAR(45) AS z_o_item_code,
        DATEADD('day', - 1, CONVERT_TIMEZONE('Asia/Tokyo', 'UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)) AS prev_insertdate
    FROM source
    WHERE order_dt >= '1-JAN-2019'
)

select * from final