{{
    config
    (
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["ise_id","ques_no","answer_seq"]
    )
}}
with sdl_vn_interface_choices as (
    select *, dense_rank() over (partition by ise_id,ques_no,answer_seq order by filename desc) rnk 
     from {{ source('vnmsdl_raw', 'sdl_vn_interface_choices') }}
    where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_choices__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_choices__duplicate_test')}}
    ) qualify rnk = 1
),
final as (
select
ise_id::varchar(255) as ise_id,
channel_id::number(20,0) as channel_id,
channel_name::varchar(255) as channel_name,
ques_no::number(20,0) as ques_no,
answer_seq::number(20,0) as answer_seq,
sku_group::varchar(255) as sku_group,
rep_param::varchar(255) as rep_param,
putup_id::varchar(255) as putup_id,
description::varchar(255) as description,
score::number(20,0) as score,
sfa::number(20,0) as sfa,
brand_id::varchar(255) as brand_id,
brand_name::varchar(255) as brand_name,
filename::varchar(255) as filename,
current_timestamp()::timestamp_ntz(9) as crt_dttm
from sdl_vn_interface_choices
)
select * from final