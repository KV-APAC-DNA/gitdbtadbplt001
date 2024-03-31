{{
    config
    (
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["ise_id","slsper_id","shop_code","cust_code","ques_no","answer_seq","createddate"]
    )
}}
with sdl_vn_interface_notes as (
    select * from {{ source('vnmsdl_raw', 'sdl_vn_interface_notes') }}
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
current_timestamp()::timestamp_ntz(9) as crt_dttm,
from sdl_vn_interface_notes
)
select * from final