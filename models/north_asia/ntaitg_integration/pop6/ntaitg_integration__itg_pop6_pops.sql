
with itg_pop6_pops_wrk as (
    select * from {{ ref('ntaitg_integration__itg_pop6_pops_wrk') }}
),

final as (
SELECT 
cntry_cd::varchar(10) as cntry_cd,
src_file_date::varchar(32) as src_file_date,
status::number(18,0) as status,
popdb_id::varchar(255) as popdb_id,
pop_code::varchar(50) as pop_code,
pop_name::varchar(100) as pop_name,
address::varchar(100) as address,
longitude::number(18,5) as longitude,
latitude::number(18,5) as latitude,
country::varchar(200) as country,
channel::varchar(200) as channel,
retail_environment_ps::varchar(200) as retail_environment_ps,
customer::varchar(200) as customer,
sales_group_code::varchar(200) as sales_group_code,
sales_group_name::varchar(200) as sales_group_name,
customer_grade::varchar(200) as customer_grade,
external_pop_code::varchar(200) as external_pop_code,
hashkey::varchar(200) as hashkey,
effective_from::timestamp_ntz(9) as effective_from,
effective_to::timestamp_ntz(9) as effective_to,
active::varchar(2) as active,
file_name::varchar(100) as file_name,
run_id::number(19,0) as run_id,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm,
business_units_id::varchar(255) as business_units_id,
business_unit_name::varchar(255) as business_unit_name,
territory_or_region::varchar(200) as territory_or_region
FROM itg_pop6_pops_wrk)
select * from final