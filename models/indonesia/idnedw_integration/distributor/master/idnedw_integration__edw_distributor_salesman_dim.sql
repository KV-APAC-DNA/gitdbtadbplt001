{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
    )
}}
with source as 
(
    select * from {{ ref('idnedw_integration__edw_distributor_ivy_user_master') }}
),
final as
(
    select 
    distinct 
       dis_code as jj_sap_dstrbtr_id,
       dis_name as jj_sap_dstrbtr_nm,
       sr_code as slsmn_id,
       sr_name as slsmn_nm,
       (dis_code||sr_code) as rec_key,
       current_timestamp()::timestamp_ntz(9) as crtd_dttm,
       current_timestamp()::timestamp_ntz(9) as updt_dttm,
       sr_code as sfa_id,
	   '200001' as effective_from,
	   '999912' as effective_to 
    from source
)
select * from final