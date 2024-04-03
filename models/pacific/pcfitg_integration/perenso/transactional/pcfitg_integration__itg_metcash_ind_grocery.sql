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
*,
current_timestamp() as update_dt
from wks_itg_metcash_ind_grocery
)
select * from final