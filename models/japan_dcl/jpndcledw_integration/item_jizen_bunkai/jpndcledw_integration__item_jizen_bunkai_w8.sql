with ITEM_JIZEN_BUNKAI_W10 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w10') }}
),

trns as
(	
    SELECT BOM.ITEMCODE AS ITEMCODE,
        SUM(BOM.SURYO) AS KOSECODE_CNT
    FROM ITEM_JIZEN_BUNKAI_W10 BOM
    GROUP BY BOM.ITEMCODE
),

final as
(
    select
        ITEMCODE::VARCHAR(40) as ITEMCODE,
        KOSECODE_CNT::NUMBER(38,4) as KOSECODE_CNT
    from trns
)

select * from final