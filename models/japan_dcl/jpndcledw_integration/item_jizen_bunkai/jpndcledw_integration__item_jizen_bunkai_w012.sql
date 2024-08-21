with ITEM_JIZEN_BUNKAI_WEND as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}
),

trns as
(	
    SELECT ITEMCODE AS ITEMCODE,
        sum(SURYO) AS SURYO
    FROM ITEM_JIZEN_BUNKAI_WEND
    GROUP BY ITEMCODE
    HAVING sum(KOSERITSU) = 0
),

final as
(
    select
        ITEMCODE::VARCHAR(20) as ITEMCODE,
	    SURYO::NUMBER(38,8) as SURYO
    from trns
)

select * from final