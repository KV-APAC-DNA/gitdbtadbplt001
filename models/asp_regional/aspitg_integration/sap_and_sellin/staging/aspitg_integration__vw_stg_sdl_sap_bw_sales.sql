with

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_sales') }}

),

final as (

    select
        *
    from source

)

select * from final
