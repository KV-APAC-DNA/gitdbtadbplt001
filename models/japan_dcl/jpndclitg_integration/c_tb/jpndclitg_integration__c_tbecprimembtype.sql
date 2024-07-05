with source as
(
    -- select * from {{ source('jpdclsdl_raw', 'c_tbecprimembtype') }}
    select * from DEV_DNA_LOAD.JPDCLSDL_RAW.C_TBECPRIMEMBTYPE
),


final as
(
    select * from source
)

select * from final