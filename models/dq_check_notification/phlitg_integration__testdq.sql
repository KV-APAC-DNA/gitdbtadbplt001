with source as (
select * from {{ source('core_integration', 'test_sdl_ph_dms_sellout_sales_fact') }})
select * from source