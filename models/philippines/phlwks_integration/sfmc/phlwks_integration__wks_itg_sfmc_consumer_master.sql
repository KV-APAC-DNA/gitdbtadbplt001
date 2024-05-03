-- depends_on: {{ ref('phlwks_integration__wks_ph_sfmc_consumer_master') }}
{{
    config(
        pre_hook="{{build_sfmc_consumer_master()}}"
    )
}}
with source as 
(
    select * from {{this}}
)
select * from source