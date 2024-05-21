{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('idnsdl_raw','sdl_distributor_ivy_merchandising') }}
),
final as (
    select 
    distributor_code,
    distributor_name,
    sales_repcode,
    sales_repname,
    channel_name,
    sub_channel_name,
    retailer_code,
    retailer_name,
    month,
    surveydate,
    aq_name,
    srd_answer,
    link,
    cdl_dttm,
    null as run_id,
    source_file_name
    from source
)

select * from final