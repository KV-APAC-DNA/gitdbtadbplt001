with item_jizen_bunkai_w06 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}
),

item_jizen_bunkai_w08 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w08') }}
),


transformed as
(
    select 
        t1.itemcode as itemcode,
        t1.kosecode as kosecode,
        t1.koseritsu as koseritsu,
        t2.koseritsukei as koseritsukei,
        t2.sa as sa
    from 
        item_jizen_bunkai_w06 t1,
	    item_jizen_bunkai_w08 t2
    where 
        t1.itemcode = t2.itemcode
),

final as
(
    select 
        itemcode::varchar(20) as itemcode,
        kosecode::varchar(20) as kosecode,
        koseritsu::number(16,8) as koseritsu,
        koseritsukei::number(16,8) as koseritsukei,
        sa::number(16,8) as sa
    from transformed
)

select * from final