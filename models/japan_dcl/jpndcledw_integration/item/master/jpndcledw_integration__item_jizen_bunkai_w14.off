with source as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w13') }}
),

transformed as
(
    select 
        bom.itemcode as itemcode,
	    sum(bom.suryo) as kosecode_cnt
    from source bom
    group by bom.itemcode
),

final as
(
    select 
        itemcode::varchar(40) as itemcode,
        kosecode_cnt::varchar(40) as kosecode_cnt
    from transformed
)

select * from final