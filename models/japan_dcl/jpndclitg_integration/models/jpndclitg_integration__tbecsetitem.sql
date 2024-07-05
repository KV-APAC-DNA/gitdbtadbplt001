with source as
(
    -- select * from {{ source('jpdclsdl_raw', 'tbecsetitem') }}
    select * from DEV_DNA_LOAD.JPDCLSDL_RAW.TBECSETITEM
),


final as
(
    select * from source
)

select * from final