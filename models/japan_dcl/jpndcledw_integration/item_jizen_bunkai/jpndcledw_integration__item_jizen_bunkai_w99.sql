with ITEM_JIZEN_BUNKAI_W3 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
),

ITEM_JIZEN_BUNKAI_W93 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w93') }}
),

trns as
(	
    SELECT TM14.ITEMCODE AS ITEMCODE,
	TM14.KOSECODE AS KOSECODE,
	TM14.SURYO AS SURYO
    FROM ITEM_JIZEN_BUNKAI_W3 TM14,
        ITEM_JIZEN_BUNKAI_W93 T1
    WHERE TM14.ITEMCODE = T1.ITEMCODE
        AND (
            TM14.KOSECODE LIKE '0083%'
            OR TM14.KOSECODE LIKE '0084%'
            OR TM14.KOSECODE LIKE '0085%'
            )
),

final as
(
    select
        ITEMCODE::VARCHAR(40) as ITEMCODE,
        KOSECODE::VARCHAR(40) as KOSECODE,
        SURYO::NUMBER(38,4) as SURYO
    from trns
)

select * from final