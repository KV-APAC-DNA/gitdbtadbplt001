with item_jizen_bunkai_w3 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
),

item_jizen_bunkai_w11 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w11') }}
),


transformed as
(
    select 
        bom.itemcode as itemcode,
        bom.kosecode as kosecode,
        bom.suryo as suryo
    from item_jizen_bunkai_w3 bom
    where 
    (
        bom.kosecode like '0081%'
        or bom.kosecode like '0086%'
    )
    and bom.itemcode not in 
    (
        select itemcode
        from item_jizen_bunkai_w11
        where sa < 0
    )
),

final as
(
    select 
        itemcode::varchar(40) as itemcode,
        kosecode::varchar(40) as kosecode,
        suryo::number(38,4) as suryo
    from transformed
)

select * from final