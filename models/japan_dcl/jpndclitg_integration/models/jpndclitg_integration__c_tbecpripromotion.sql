with source as
(
    -- select * from {{ source('jpdclsdl_raw', 'c_tbecpripromotion') }}
    select * from DEV_DNA_LOAD.JPDCLSDL_RAW.C_TBECPRIPROMOTION
),


final as
(
    select * from source
)

select * from final