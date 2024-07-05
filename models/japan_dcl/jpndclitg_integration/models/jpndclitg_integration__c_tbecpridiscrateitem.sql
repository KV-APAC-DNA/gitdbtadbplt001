with source as
(
    --select * from {{ source('jpdclsdl_raw', 'c_tbecpridiscrateitem') }}
    select * from dev_dna_load.jpdclsdl_raw.c_tbecpridiscrateitem
),


final as
(
    select * from source
)

select * from final