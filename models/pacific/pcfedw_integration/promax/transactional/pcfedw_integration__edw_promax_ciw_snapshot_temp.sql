{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{{build_edw_promax_ciw_snapshot()}}"
    )
}}
with source as
(
    select * from snappcfwks_integration.wks_promax_ciw_snapshot_current
),
final as
(
    select * from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where (snapshot_date)::date > (select max(snapshot_date)::date from {{ this }}) 
    {% endif %}

)
select * from final
