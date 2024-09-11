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
    select *, dense_rank() over (partition by slsper_id,branch_code,visitdate order by filename desc) rnk
    from {{ source('vnmsdl_raw', 'sdl_vn_interface_cpg') }}
    where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_cpg__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_cpg__duplicate_test')}}
    ) qualify rnk = 1
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