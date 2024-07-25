--{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where DATEDIFF(day,crt_dttm,current_timestamp())>='90';
        {% endif %}"
    )
}}

with itg_pvc_input_content as
(
    select * from {{ ref('aspitg_integration__itg_pvc_input_content') }}
),
final as
(
    SELECT matl_num,
        content,
        content_short,
        content_number,
        convert_timezone('UTC',current_timestamp()) as crt_dttm
    FROM itg_pvc_input_content
)
select matl_num::varchar(18) as matl_num,
    content::varchar(50) as content,
    content_short::varchar(50) as content_short,
    content_number::number(20,5) as content_number,
    crt_dttm::timestamp_ntz(9) as crt_dttm
 from final