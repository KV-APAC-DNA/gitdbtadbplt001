with source as
(
    -- select * from {{ source('jpdclsdl_raw', 'c_tbmembunitrel') }}
    select * from dev_dna_load.jpdclsdl_raw.c_tbmembunitrel
),


final as
(
    select * from source
)

select * from final