{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["code"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where (trim(code)) in (select distinct trim(code) from {{ source('idnsdl_raw', 'sdl_mds_id_ref_customerpl_segmentation_adftemp') }});
        {% endif %}"
    )
}}
with source as  
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_ref_customerpl_segmentation_adftemp') }}
),
final as 
(
    select
    trim(code)::varchar(500) as code,
	trim(customer_segment_level1_code)::varchar(500) as customer_segment_level1,
	trim(customer_segment_level2_code)::varchar(500) as customer_segment_level2 
    from source
)
select * from final