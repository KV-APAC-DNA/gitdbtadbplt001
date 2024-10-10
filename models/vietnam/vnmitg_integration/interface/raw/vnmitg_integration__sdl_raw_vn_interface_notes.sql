
{{
    config(
        materialized="incremental",
        incremental_strategy="append"
        )
}}

with source as (
    select * from {{ source('vnmsdl_raw', 'sdl_vn_interface_notes') }}
    where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_notes__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_notes__duplicate_test')}}
    ) 
),
final as (
select 
cust_code::varchar(255) as cust_code,
slsper_id::varchar(255) as slsper_id,
shop_code::varchar(255) as shop_code,
ise_id::varchar(255) as ise_id,
ques_no::number(10,0) as ques_no,
answer_seq::number(20,0) as answer_seq,
answer_value::varchar(255) as answer_value,
createddate::varchar(255) as createddate,
filename::varchar(255) as filename,
crt_dttm::timestamp_ntz(9) as crt_dttm
from  source
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)
select * from final

