{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key = ["slsper_id","branch_code","visitdate"]
        )
}}
--Import CTE
with source as 
(
    select *
    from {{ source('vnmsdl_raw', 'sdl_vn_interface_cpg') }}
),
--Final CTE
final as (
    select  
            slsper_id,
            branch_code,
            createddate,
            visitdate,
            filename,
            convert_timezone('Asia/Singapore',current_timestamp)::timestamp_ntz(9) AS crt_dttm
    from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)

--Final select
select * from final