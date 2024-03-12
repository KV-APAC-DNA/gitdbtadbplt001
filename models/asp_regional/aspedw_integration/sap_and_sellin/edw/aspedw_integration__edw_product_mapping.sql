with source as (
    select * from {{ ref('aspitg_integration__itg_product_mapping') }}
)


select * from source


