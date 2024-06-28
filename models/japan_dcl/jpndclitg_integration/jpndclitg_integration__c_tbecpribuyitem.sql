with source as(
    select * from DEV_DNA_LOAD.JPDCLSDL_RAW.c_tbecpribuyitem
),
final as(
    select * from source
)
select * from final