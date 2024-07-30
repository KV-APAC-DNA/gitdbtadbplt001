with source as
(
    select * from {{ source('jpnsdl_raw', 'edi_excl_rtlr') }}
),

final as
(
    select * from source
)

select * from final