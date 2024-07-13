with itg_perenso_head_office_req_check as (
    select * from {{ ref('pcfitg_integration__itg_perenso_head_office_req_check') }}
),
itg_perenso_head_office_req_state as (
    select * from {{ ref('pcfitg_integration__itg_perenso_head_office_req_state') }}
),
itg_perenso_store_chk_hdr as (
    select * from {{ ref('pcfitg_integration__itg_perenso_store_chk_hdr') }}
),
itg_perenso_diary_item as (
       select * from {{ ref('pcfitg_integration__itg_perenso_diary_item') }}
),
itg_perenso_diary_item_type as (
       select * from {{ ref('pcfitg_integration__itg_perenso_diary_item_type') }}
),
itg_perenso_work_item as (
       select * from {{ ref('pcfitg_integration__itg_perenso_work_item') }}
),
itg_perenso_todo as (
       select * from {{ ref('pcfitg_integration__itg_perenso_todo') }}
),
itg_perenso_constants as (
       select * from {{ ref('pcfitg_integration__itg_perenso_constants') }}
),
transformed as (
select 'Compliance' as perenso_source,

       'Compliance' as todo_type,  --ipc1.const_desc as todo_type,

       iphorc.store_chk_hdr_key,

       ipsch.store_chk_date,

       iphors.acct_key,

       iphorc.prod_grp_key,

       ipdi.create_user_key as create_user_key,

       ipt.todo_desc,

       ipt.start_date as to_do_start_date,

       ipt.end_date as to_do_end_date,

       iphorc.fail_reason_desc as fail_reason_desc,

       iphors.start_date as req_start_date,

       iphors.end_date as req_end_date,

       ipc2.const_desc as req_state,

       ipdit.diary_item_type_desc,

       ipc3.const_desc as diary_item_category,

       ipdi.start_time as diary_start_time,

       ipdi.end_time as diary_end_time,

       ipdi.complete as diary_complete,

       ipdit.count_in_call_rate,

       ipwi.work_item_desc,

       ipc4.const_desc as work_item_type,

       ipwi.start_date as work_item_start_date,

       ipwi.end_date as work_item_end_date      

from itg_perenso_head_office_req_check iphorc,

     itg_perenso_head_office_req_state iphors,

     itg_perenso_store_chk_hdr ipsch,

     itg_perenso_diary_item ipdi,

     itg_perenso_diary_item_type ipdit,

     itg_perenso_work_item ipwi,

     itg_perenso_todo ipt,

     itg_perenso_constants ipc1,

     itg_perenso_constants ipc2,

     itg_perenso_constants ipc3,

     itg_perenso_constants ipc4

where iphorc.store_chk_hdr_key = iphors.store_chk_hdr_key(+)

and   iphorc.prod_grp_key = iphors.prod_grp_key(+)

and   iphorc.todo_key = iphors.todo_key(+)

and   iphorc.store_chk_hdr_key = ipsch.store_chk_hdr_key

and   ipsch.diary_item_key = ipdi.diary_item_key

--and   iphors.acct_key = ipdi.acct_key

and   ipdi.diary_item_type_key = ipdit.diary_item_type_key

and   ipsch.work_item_key = ipwi.work_item_key

and   iphorc.todo_key = ipt.todo_key

--and   ipsch.work_item_key = ipt.work_item_key

and   ipt.todo_type = ipc1.const_key(+)

and   iphors.req_state = ipc2.const_key(+)

and   ipdit.category = ipc3.const_key(+)

and   ipwi.work_item_type = ipc4.const_key(+)),
final as (
select
perenso_source::varchar(10) as perenso_source,
todo_type::varchar(10) as todo_type,
store_chk_hdr_key::number(10,0) as store_chk_hdr_key,
store_chk_date::timestamp_ntz(9) as store_chk_date,
acct_key::number(10,0) as acct_key,
prod_grp_key::number(10,0) as prod_grp_key,
create_user_key::number(10,0) as create_user_key,
todo_desc::varchar(255) as todo_desc,
to_do_start_date::timestamp_ntz(9) as to_do_start_date,
to_do_end_date::timestamp_ntz(9) as to_do_end_date,
fail_reason_desc::varchar(256) as fail_reason_desc,
req_start_date::timestamp_ntz(9) as req_start_date,
req_end_date::timestamp_ntz(9) as req_end_date,
req_state::varchar(255) as req_state,
diary_item_type_desc::varchar(255) as diary_item_type_desc,
diary_item_category::varchar(255) as diary_item_category,
diary_start_time::timestamp_ntz(9) as diary_start_time,
diary_end_time::timestamp_ntz(9) as diary_end_time,
diary_complete::varchar(5) as diary_complete,
count_in_call_rate::varchar(5) as count_in_call_rate,
work_item_desc::varchar(255) as work_item_desc,
work_item_type::varchar(255) as work_item_type,
work_item_start_date::timestamp_ntz(9) as work_item_start_date,
work_item_end_date::timestamp_ntz(9) as work_item_end_date
from transformed
)

select * from final
