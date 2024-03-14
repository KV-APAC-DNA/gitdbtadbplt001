{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('sgpsdl_raw','sdl_sg_ciw_mapping') }}
),
final as(
    select 
        condition_type,
        gl,
        gl_description,
        posted_where,
        purpose,
        ciw_bucket,
        null as cdl_dttm,
        curr_date,
        file_name,
        run_id
    from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.curr_date > (select max(curr_date) from {{ this }}) 
 {% endif %}
)

select * from final