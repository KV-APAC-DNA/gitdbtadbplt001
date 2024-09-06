with ITEM_JIZEN_BUNKAI_W3 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
),


trns as
(	
    SELECT TM14.ITEMCODE AS ITEMCODE,
	COUNT(*) AS CNT
    FROM ITEM_JIZEN_BUNKAI_W3 TM14
    GROUP BY TM14.ITEMCODE
),

final as
(
    select
        ITEMCODE::VARCHAR(40) as ITEMCODE,
	    CNT::NUMBER(38,0) as CNT
    from trns
)

select * from final