{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['week_end_dt'],
        pre_hook= "delete from {{this}} where week_end_dt in (select distinct week_end_dt from {{ ref('pcfwks_integration__wks_itg_metcash_ind_grocery') }} );""
"
    )
}}

with wks_itg_metcash_ind_grocery as (
    select * from {{ ref('pcfwks_integration__wks_itg_metcash_ind_grocery') }}
),
final as (
select
week_end_dt::date as week_end_dt,
supp_id::varchar(20) as supp_id,
supp_name::varchar(50) as supp_name,
state::varchar(50) as state,
banner_id::varchar(10) as banner_id,
banner::varchar(50) as banner,
customer_id::varchar(20) as customer_id,
customer::varchar(50) as customer,
product_id::varchar(20) as product_id,
product::varchar(50) as product,
gross_sales::float as gross_sales,
gross_cases::float as gross_cases,
file_name::varchar(100) as file_name,
run_id::varchar(50) as run_id,
create_dt::timestamp_ntz(9) as create_dt,
current_timestamp()::timestamp_ntz(9) as update_dt
from wks_itg_metcash_ind_grocery
)
select * from final