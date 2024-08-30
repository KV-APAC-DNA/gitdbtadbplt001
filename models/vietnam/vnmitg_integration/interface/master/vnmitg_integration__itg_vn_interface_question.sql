{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key = ["ise_id","ques_no"]
        )
}}
--Import CTE
with source as 
(
    select * , dense_rank() over (partition by ise_id,ques_no order by filename desc) rnk
    from {{ source('vnmsdl_raw', 'sdl_vn_interface_question') }}
    where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_question__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_question__duplicate_test')}}
        )
        qualify rnk = 1
),
--Final CTE
final as (
    select  
            ise_id,
            channel_id,
            channel,
            ques_no,
            ques_code,
            ques_desc,
            standard_ques,
            ques_class_code,
            ques_class_desc,
            weigh,
            total_score,
            answer_code,
            answer_desc,
            franchise_code,
            franchise_name,
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