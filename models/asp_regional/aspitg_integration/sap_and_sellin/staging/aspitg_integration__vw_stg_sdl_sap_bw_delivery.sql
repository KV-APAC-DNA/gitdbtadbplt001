with source as(
    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_delivery') }}
)