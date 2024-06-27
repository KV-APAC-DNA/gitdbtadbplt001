--REQ000000180060: SSR O&A display points content change
with itg_perenso_over_and_above_state as (
    select * from {{ ref('pcfitg_integration__itg_perenso_over_and_above_state') }}
),
itg_perenso_over_and_above as (
    select * from {{ ref('pcfitg_integration__itg_perenso_over_and_above') }}
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
itg_perenso_todo_option as (
    select * from {{ ref('pcfitg_integration__itg_perenso_todo_option') }}
),
itg_perenso_todo as (
    select * from {{ ref('pcfitg_integration__itg_perenso_todo') }}
),
itg_perenso_constants as (
    select * from {{ ref('pcfitg_integration__itg_perenso_constants') }}
),
itg_perenso_instore_cycle_dates as (
select * from {{ ref('pcfitg_integration__itg_perenso_instore_cycle_dates') }}
),
ipicd as (select  "TIME", 

        cycle_year,

        to_date((substring(ipicd.start_date,7,4) || '-' ||

                 substring(ipicd.start_date,4,2) || '-' ||

                 substring(ipicd.start_date,1,2)),'YYYY-MM-DD') as start_date,

        to_date((substring(ipicd.end_date,7,4) || '-' ||

                 substring(ipicd.end_date,4,2) || '-' ||

                 substring(ipicd.end_date,1,2)),'YYYY-MM-DD') as end_date

from itg_perenso_instore_cycle_dates ipicd),
itg_perenso_over_and_above_points as (
select * from {{ ref('pcfitg_integration__itg_perenso_over_and_above_points') }}
),
transformed as (
select 'Over & Above' as perenso_source,

		'Over & Above' as todo_type,		--IPC1.CONST_DESC AS TODO_TYPE,

       ipto.option_desc as to_do_option_desc,

       ipoas.store_chk_hdr_key,

       ipsch.store_chk_date,

       ipoa.acct_key as oa_acct_key,

       ipoa.prod_grp_key as prod_grp_key,

       ipdi.create_user_key as create_user_key,

       ipoas.over_and_above_key,

       ipoas.start_date as oa_start_date,

       ipoas.end_date as oa_end_date,

       ipoas.batch_count,

       ipt.todo_desc,

       ipt.start_date as to_do_start_date,

       ipt.end_date as to_do_end_date,

       ipoa.activated,

       ipoa.notes as over_and_above_notes,

       ipsch.acct_key as st_chk_acct_key,

       ipdit.diary_item_type_desc,

       ipc3.const_desc as diary_item_category,

       ipdi.start_time as diary_start_time,

       ipdi.end_time as diary_end_time,

       ipdi.complete as diary_complete,

       ipdit.count_in_call_rate,

       ipwi.work_item_desc,

       ipc4.const_desc as work_item_type,

       ipwi.start_date as work_item_start_date,

       ipwi.end_date as work_item_end_date,

       ipicd.start_date as cycle_start_date,

       ipicd.end_date as cycle_end_date,

       ipicd.time as cycle_number,

	   ioaap.points as oa_points

from itg_perenso_over_and_above_state ipoas,

     itg_perenso_over_and_above ipoa,

     itg_perenso_store_chk_hdr ipsch,

     itg_perenso_diary_item ipdi,

     itg_perenso_diary_item_type ipdit,

     itg_perenso_work_item ipwi,

     itg_perenso_todo_option ipto,

     itg_perenso_todo ipt,

     itg_perenso_constants ipc1,

     itg_perenso_constants ipc3,

     itg_perenso_constants ipc4,

	       ipicd,

	 itg_perenso_over_and_above_points ioaap	

where ipoas.over_and_above_key = ipoa.over_and_above_key

and   ipoas.store_chk_hdr_key = ipsch.store_chk_hdr_key

and   ipsch.diary_item_key = ipdi.diary_item_key

and   ipsch.acct_key = ipdi.acct_key

and   ipdi.diary_item_type_key = ipdit.diary_item_type_key

and   ipsch.work_item_key = ipwi.work_item_key

and   ipoa.todo_option_key = ipto.option_key

and   ipto.todo_key = ipt.todo_key

and   ipt.todo_type = ipc1.const_key(+)

and   ipdit.category = ipc3.const_key(+)

and   ipwi.work_item_type = ipc4.const_key(+)

and   ipdi.start_time::date >= ipicd.start_date(+)::date

and   ipdi.start_time::date <= ipicd.end_date(+)::date

and   upper(ipto.option_desc) = upper(ioaap.OA_display_type(+))
),
final as (
select 
perenso_source::varchar(12) as perenso_source,
todo_type::varchar(12) as todo_type,
to_do_option_desc::varchar(256) as to_do_option_desc,
store_chk_hdr_key::number(10,0) as store_chk_hdr_key,
store_chk_date::timestamp_ntz(9) as store_chk_date,
oa_acct_key::number(10,0) as oa_acct_key,
prod_grp_key::number(10,0) as prod_grp_key,
create_user_key::number(10,0) as create_user_key,
over_and_above_key::number(10,0) as over_and_above_key,
oa_start_date::timestamp_ntz(9) as oa_start_date,
oa_end_date::timestamp_ntz(9) as oa_end_date,
batch_count::number(10,0) as batch_count,
todo_desc::varchar(255) as todo_desc,
to_do_start_date::timestamp_ntz(9) as to_do_start_date,
to_do_end_date::timestamp_ntz(9) as to_do_end_date,
activated::varchar(10) as activated,
over_and_above_notes::varchar(255) as over_and_above_notes,
st_chk_acct_key::number(10,0) as st_chk_acct_key,
diary_item_type_desc::varchar(255) as diary_item_type_desc,
diary_item_category::varchar(255) as diary_item_category,
diary_start_time::timestamp_ntz(9) as diary_start_time,
diary_end_time::timestamp_ntz(9) as diary_end_time,
diary_complete::varchar(5) as diary_complete,
count_in_call_rate::varchar(5) as count_in_call_rate,
work_item_desc::varchar(255) as work_item_desc,
work_item_type::varchar(255) as work_item_type,
work_item_start_date::timestamp_ntz(9) as work_item_start_date,
work_item_end_date::timestamp_ntz(9) as work_item_end_date,
cycle_start_date::date as cycle_start_date,
cycle_end_date::date as cycle_end_date,
cycle_number::varchar(20) as cycle_number,
oa_points::number(10,3) as oa_points
from transformed
)
select * from final
