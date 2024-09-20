with ITEM_ZAIKO_tbl as
(
    select * from {{ ref('jpndcledw_integration__item_zaiko_tbl') }} 
),

trns as
(
    SELECT 
        z_itemcode AS ITEMCODE,
        TANKA						
    FROM ITEM_ZAIKO_tbl --在庫の商品情報のみの取得						
    where marker in (1,2)
    GROUP BY z_itemcode,TANKA	
),

final as
(
    select
        ITEMCODE::VARCHAR(40) as ITEMCODE,
        TANKA::NUMBER(16,8) as TANKA
    from trns
)

select * from final