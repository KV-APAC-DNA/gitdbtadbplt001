-- alter table au_edw.edw_perenso_compliance

-- add column compliance_target varchar(2);



-- alter table au_edw.edw_perenso_compliance

-- add column compliance_promo_group varchar(100);



-- alter table au_edw.edw_perenso_compliance

-- drop column compliance_target;



-- alter table au_edw.edw_perenso_compliance

-- drop column compliance_promo_group;
with itg_perenso_head_office_req_check as (
       select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_HEAD_OFFICE_REQ_CHECK
),
itg_perenso_head_office_req_state as (
       select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_HEAD_OFFICE_REQ_STATE
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
itg_perenso_todo as (
       select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_TODO
),
itg_perenso_constants as (
       select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_CONSTANTS
),
final as (
select 'compliance' as perenso_source,

       'compliance' as todo_type,  --ipc1.const_desc as todo_type,

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

and   ipwi.work_item_type = ipc4.const_key(+))

select * from final



--commit;



