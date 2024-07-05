with source as
(
    --select * from {{ source('jpdclsdl_raw', 'c_tbecpridiscratecategory') }}
    select * from dev_dna_load.jpdclsdl_raw.c_tbecpridiscratecategory
),


final as
(
    select * from source
)

select * from final