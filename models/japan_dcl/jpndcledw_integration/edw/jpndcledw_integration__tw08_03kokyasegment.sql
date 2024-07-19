with tw08_02konyusihyokbn as (
    select * from dev_dna_core.snapjpdcledw_integration.tw08_02konyusihyokbn
),
tm28rfmsegment as (
    select * from dev_dna_core.snapjpdcledw_integration.tm28rfmsegment
),
transformed as (
    select
        f2.saleno             as saleno
        ,ifnull(segkbncode,'00')  as segkbncode
    from 
        tw08_02konyusihyokbn f2
    left join tm28rfmsegment m28
        on  m28.rkbncode = f2.konyurkbncode
        and m28.fkbncode = f2.nenfkbncode
        and m28.mkbncode = f2.ruimkbncode
),
final as (
    select
        saleno::varchar(18) as saleno,
        segkbncode::varchar(3) as segkbncode,   
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final
