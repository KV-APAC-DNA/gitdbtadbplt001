with ITEM_JIZEN_BUNKAI_WEND1 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_wend1') }}
),

trns as
(	
    SELECT W14.ITEMCODE AS ITEMCODE,
        W14.KOSECODE AS KOSECODE,
        W14.SURYO AS SURYO,
        W14.KOSERITSU AS KOSERITSU,
        CAST(TO_CHAR(current_timestamp(), 'YYYYMMDD') AS NUMERIC) AS INSERTDATE,
        cast(TO_CHAR(current_timestamp(), 'HH24MISS') AS NUMERIC) AS INSERTTIME,
        'DWH401' AS INSERTID,
        BUNKAIKBN AS BUNKAIKBN,
        1 AS MARKER
    FROM ITEM_JIZEN_BUNKAI_WEND1 W14
),

final as
(
    select
        ITEMCODE::VARCHAR(45) as ITEM_CD,
        KOSECODE::VARCHAR(45) as KOSEI_CD,
        SURYO::NUMBER(13,4) as SURYO,
        KOSERITSU::NUMBER(16,8) as KOSERITSU,
        INSERTDATE::NUMBER(18,0) as INSERTDATE,
        INSERTTIME::NUMBER(18,0) as INSERTTIME,
        INSERTID::VARCHAR(9) as INSERTID,
        BUNKAIKBN::VARCHAR(1) as BUNKAIKBN,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as INSERTED_DATE,
        NULL::VARCHAR(100) as INSERTED_BY,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as UPDATED_DATE,
        NULL::VARCHAR(100) as UPDATED_BY,
        MARKER::NUMBER(38,0) as MARKER,
    from trns
)

select * from final