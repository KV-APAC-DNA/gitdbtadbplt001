{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        post_hook="delete from 
                    {% if target.name=='prod' %}
                        pcfedw_integration.edw_demand_forecast_snapshot
                    {% else %}
                        {{schema}}.pcfedw_integration__edw_demand_forecast_snapshot
                    {% endif %}
                    where replace(left(snap_shot_dt,7),'-','') < replace(left(dateadd(month, -6, current_timestamp),7),'-','');
                    create or replace table 
                    {% if target.name=='prod' %}
                        pcfedw_integration.edw_demand_forecast_snapshot_temp
                    {% else %}
                        {{schema}}.pcfedw_integration__edw_demand_forecast_snapshot_temp
                    {% endif %}
                clone {% if target.name=='prod' %}
                        pcfedw_integration.edw_demand_forecast_snapshot
                    {% else %}
                        {{schema}}.pcfedw_integration__edw_demand_forecast_snapshot
                    {% endif %}; "
    )
}}
with source as (
    select * from {{ ref('pcfedw_integration__edw_demand_forecast_snapshot') }}
),
final as (
    select * from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where snap_shot_dt > (select max(snap_shot_dt) from {{ this }}) 
    {% endif %}
)
select * from final