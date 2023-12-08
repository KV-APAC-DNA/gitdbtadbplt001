{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ ref('itg_sg_scan_data_marketplace') }}
)

select * from source