{{
    config(
        materialized="incremental",
        incremental_strategy="append",
       post_hook="{{sap_transaction_processed_files('BWA_CDL_DELIVERY','vw_stg_sdl_sap_bw_delivery','sdl_raw_sap_bw_delivery')}}"
    )}}

with source as(
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_delivery') }}
),
final as(
    select * from source
    where not exists (
    select 
        act_file_name 
    from sap_transactional_processed_files 
    where target_table_name='sdl_raw_sap_bw_delivery' and sap_transactional_processed_files.act_file_name=source.filename
  )
)

select * from final