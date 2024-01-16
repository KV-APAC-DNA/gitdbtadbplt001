with

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_billing') }}

),

final as (

    select
        *
    from source

)

select * from final
