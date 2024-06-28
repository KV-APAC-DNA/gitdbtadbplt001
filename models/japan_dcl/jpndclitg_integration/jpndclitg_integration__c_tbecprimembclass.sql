with source as(
    select * from DEV_DNA_LOAD.JPDCLSDL_RAW.C_TBECPRIMEMBCLASS
),
final as(
    select * from source
)
select * from final