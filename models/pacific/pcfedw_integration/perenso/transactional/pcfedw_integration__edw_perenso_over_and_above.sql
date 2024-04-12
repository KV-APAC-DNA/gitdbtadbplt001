-- ALTER TABLE AU_EDW.EDW_PERENSO_OVER_AND_ABOVE

-- ADD COLUMN CYCLE_START_DATE DATE;

-- 

-- ALTER TABLE AU_EDW.EDW_PERENSO_OVER_AND_ABOVE

-- ADD COLUMN CYCLE_END_DATE DATE;



-- ALTER TABLE AU_EDW.EDW_PERENSO_OVER_AND_ABOVE

-- ADD COLUMN CYCLE_NUMBER VARCHAR(20);



-- ALTER TABLE AU_EDW.EDW_PERENSO_OVER_AND_ABOVE

-- ADD COLUMN OA_POINTS NUMERIC(10,3);

with itg_perenso_over_and_above_state as (
    select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_OVER_AND_ABOVE_STATE
),
itg_perenso_over_and_above as (
    select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_OVER_AND_ABOVE
),
itg_perenso_store_chk_hdr as (
    select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_STORE_CHK_HDR
),
itg_perenso_diary_item as (
    select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_DIARY_ITEM
),
itg_perenso_diary_item_type as (
select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_DIARY_ITEM_TYPE
),
itg_perenso_work_item as (
    select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_WORK_ITEM
),
itg_perenso_todo_option as (
    select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_TODO_OPTION
),
itg_perenso_todo as (
    select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_TODO
),
itg_perenso_constants as (
    select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_CONSTANTS
),
itg_perenso_instore_cycle_dates as (
select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_INSTORE_CYCLE_DATES
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
select * from 
),
final as (
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

and   trunc(ipdi.start_time) >= ipicd.start_date(+)

and   trunc(ipdi.start_time) <= ipicd.end_date(+)

and   upper(ipto.option_desc) = upper(ioaap.oa_display_type(+))
)
select * from final





--COMMIT;