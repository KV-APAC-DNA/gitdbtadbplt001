{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        post_hook="{{sap_transaction_processed_files('BWA_CDL_SALES','vw_stg_sdl_sap_bw_sales','sdl_raw_sap_bw_sales')}}"
    )}}

with source as (
     select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_sales') }} 
),

sap_transactional_processed_files as (
    select * from {{ source('aspwks_integration', 'sap_transactional_processed_files') }}
),
final as (
    select * from source
    where not exists (
    select 
        act_file_name 
    from sap_transactional_processed_files 
    where target_table_name='sdl_raw_sap_bw_sales' and sap_transactional_processed_files.act_file_name=source.file_name
  )
)

select * from final