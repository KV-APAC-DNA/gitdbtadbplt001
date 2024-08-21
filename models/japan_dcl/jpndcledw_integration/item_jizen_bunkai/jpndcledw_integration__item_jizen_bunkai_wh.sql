with ITEM_HANBAI_tbl as
(
    select * from {{ ref('jpndcledw_integration__item_hanbai_tbl') }}
),

trns as
(	
    SELECT 
        h_itemcode AS ITEMCODE,						
        TANKA						
    FROM ITEM_HANBAI_tbl						
    WHERE  marker in (1,2,3) ----販売の商品情報のみの取得						
    GROUP BY h_itemcode,TANKA	
),

final as
(
    select
        ITEMCODE::VARCHAR(40) as ITEMCODE,
        TANKA::NUMBER(16,8) as TANKA
    from trns
)

select * from final