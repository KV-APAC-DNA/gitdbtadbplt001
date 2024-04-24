
{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        prehook="delete from {{this}} where time_period 
        in (select distinct week_ending_date from {{ ref('pcfitg_integration__sdl_pharm_sellout_weekly_raw') }})"
    )
}}

with source as (
    select * from {{ ref('pcfitg_integration__sdl_pharm_sellout_weekly_raw') }}
),
final as
(
select 
    outlet_number::varchar(10) as store_probe_id,
	name::varchar(30) as store_name,
	pfc::varchar(10) as product_probe_id,
	pack_long_desc::varchar(60) as product_description,
	week_ending_date::varchar(8) as time_period,
	units::number(18,0) as units,
	amount::number(18,6) as src_amt_value,
	amount::number(18,6) as amount,
	derived_price::number(12,6) as derived_price,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)
select * from final