with source as(
    select * from DEV_DNA_LOAD.JPDCLSDL_RAW.tbpromotioncate
),
final as(
    select * from source
)
select * from final