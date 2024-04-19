-- ALTER TABLE EDW_PERENSO_SURVEY
-- ADD COLUMN CYCLE_START_DATE DATE;
-- 
-- ALTER TABLE EDW_PERENSO_SURVEY
-- ADD COLUMN CYCLE_END_DATE DATE;

-- ALTER TABLE EDW_PERENSO_SURVEY
-- ADD COLUMN CYCLE_NUMBER VARCHAR(25);
-- 
-- ALTER TABLE EDW_PERENSO_SURVEY
-- ADD COLUMN TARGET_TYPE VARCHAR(25);

-- ALTER TABLE EDW_PERENSO_SURVEY
-- ADD COLUMN TARGET NUMERIC(10);
-- 

-- ALTER TABLE EDW_PERENSO_SURVEY
-- ADD COLUMN SURVEY_CATEGORY VARCHAR(100);


with itg_perenso_survey_result as (
     select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_SURVEY_RESULT
),
itg_perenso_store_chk_hdr as (
     select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_STORE_CHK_HDR
),
itg_perenso_todo_option as (
     select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_TODO_OPTION
),
itg_perenso_todo as (
     select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_TODO
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
itg_perenso_constants as (
     select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_CONSTANTS
),
itg_perenso_instore_cycle_dates as (
     select distinct * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_INSTORE_CYCLE_DATES
),
edw_perenso_survey_target as (
     select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_SURVEY_TARGET
),
edw_perenso_account_dim as (
     select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_ACCOUNT_DIM
),
itg_perenso_product_group as (
     select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_PRODUCT_GROUP
),
transformed as (
 select  act.*,
     target_type,
     target,
	category as survey_category
     
from
(select case when ipt.cascade_todo_key is null then 'Survey' else 'Cascading Survey' end  as perenso_source,
       case when ipt.cascade_todo_key is null then 'Survey' else 'Cascading Survey' end todo_type,--IPC1.CONST_DESC AS TODO_TYPE,
       ipsr.store_chk_hdr_key,
       ipsch.store_chk_date,
       ipsr.line_key,
       ipsch.acct_key,
       ipsr.prod_grp_key,
       ipdi.create_user_key as create_user_key,
       ipt.todo_desc,
       ipt.start_date as to_do_start_date,
       ipt.end_date as to_do_end_date,
       ipto.option_desc as to_do_option_desc,
	     ipsr.notesans as survery_notes,
       ipdit.diary_item_type_desc,
       ipc2.const_desc as diary_item_category,
       ipdi.start_time as diary_start_time,
       ipdi.end_time as diary_end_time,
       ipdi.complete as diary_complete,
       ipdit.count_in_call_rate,
       ipwi.work_item_desc,
       ipc3.const_desc as work_item_type,
       ipwi.start_date as work_item_start_date,
       ipwi.end_date as work_item_end_date,
       ipicd.start_date as cycle_start_date,
       ipicd.end_date as cycle_end_date,
       ipicd.time as cycle_number,
       parent_question.parent_ques_desc,
       case when  ipt.cascade_todo_key is null then null else ipt.dsp_order end as ques_order
from itg_perenso_survey_result ipsr,
     itg_perenso_store_chk_hdr ipsch,
     itg_perenso_todo_option ipto,
     itg_perenso_todo ipt,
     itg_perenso_diary_item ipdi,
     itg_perenso_diary_item_type ipdit,
     itg_perenso_work_item ipwi,
     itg_perenso_constants ipc1,
     itg_perenso_constants ipc2,
     itg_perenso_constants ipc3,
     (select todo_key ,todo_desc as parent_ques_desc from itg_perenso_todo where todo_key in (select distinct cascade_todo_key from itg_perenso_todo) ) parent_question , 
(select  "TIME", 
        cycle_year,
        to_date((substring(ipicd.start_date,7,4) || '-' ||
                 substring(ipicd.start_date,4,2) || '-' ||
                 substring(ipicd.start_date,1,2)),'YYYY-MM-DD') as start_date,
        to_date((substring(ipicd.end_date,7,4) || '-' ||
                 substring(ipicd.end_date,4,2) || '-' ||
                 substring(ipicd.end_date,1,2)),'YYYY-MM-DD') as end_date
from itg_perenso_instore_cycle_dates ipicd) ipicd
where ipsr.store_chk_hdr_key = ipsch.store_chk_hdr_key(+)
and   ipsr.todo_key = ipto.todo_key(+)
and   ipsr.optionans = ipto.option_key(+)
and   ipsr.todo_key = ipt.todo_key(+)
and   ipsch.diary_item_key = ipdi.diary_item_key(+)
and   ipsch.acct_key = ipdi.acct_key(+)
and   ipdi.diary_item_type_key = ipdit.diary_item_type_key
and   ipsch.work_item_key = ipwi.work_item_key
and   ipt.todo_type = ipc1.const_key(+)
and   ipdit.category = ipc2.const_key(+)
and   ipwi.work_item_type = ipc3.const_key(+)
and   ipdi.start_time >= ipicd.start_date(+)
and   ipdi.start_time <= ipicd.end_date(+)
and   ipt.cascade_todo_key = parent_question.todo_key(+))act,
(select epst.*,
       epad.acct_id,
      ippg.prod_grp_key
from (select year,
             to_date((substring(cycle_start_date,7,4) || '-' ||
                      substring(cycle_start_date,4,2) || '-' ||
                      substring(cycle_start_date,1,2)),'YYYY-MM-DD') as cycle_start_date,
             to_date((substring(cycle_end_date,7,4) || '-' ||
                      substring(cycle_end_date,4,2) || '-' ||
                      substring(cycle_end_date,1,2)),'YYYY-MM-DD') as cycle_end_date,
             account_type_description,
             target_type,
             category,
             territory_region,
             territory,
             question_product_group,
             perenso_questions,
             target
      from edw_perenso_survey_target) epst,
     (select epad.*,
          case 
            when upper(epad.acct_ind_groc_state) != 'NOT ASSIGNED' then upper(epad.acct_ind_groc_state) 
            when upper(epad.acct_au_pharma_state) != 'NOT ASSIGNED' then upper(epad.acct_au_pharma_state) 
            when upper(epad.acct_nz_pharma_state) != 'NOT ASSIGNED' then upper(epad.acct_nz_pharma_state) 
            when upper(epad.acct_nz_groc_state) != 'NOT ASSIGNED' then upper(epad.acct_nz_groc_state) 
            when upper(epad.acct_ssr_team_leader) != 'NOT ASSIGNED' then upper(epad.acct_ssr_team_leader) 
            else 'NOT ASSIGNED' 
       end territory_region,
       case 
            when upper(epad.acct_ind_groc_territory) != 'NOT ASSIGNED' then upper(epad.acct_ind_groc_territory) 
            when upper(epad.acct_au_pharma_territory) != 'NOT ASSIGNED' then upper(epad.acct_au_pharma_territory) 
            when upper(epad.acct_nz_pharma_territory) != 'NOT ASSIGNED' then upper(epad.acct_nz_pharma_territory) 
            when upper(epad.acct_nz_groc_territory) != 'NOT ASSIGNED' then upper(epad.acct_nz_groc_territory) 
            when upper(epad.acct_ssr_territory) != 'NOT ASSIGNED' then upper(epad.acct_ssr_territory) 
            when upper(epad.acct_au_pharma_ssr_territory) != 'NOT ASSIGNED' then upper(epad.acct_au_pharma_ssr_territory) 
            else 'NOT ASSIGNED' 
       end territory from edw_perenso_account_dim epad) epad,
     itg_perenso_product_group ippg
where upper(epst.territory_region) = upper(epad.territory_region(+))
and   upper(epst.territory) = upper(epad.territory(+))
and   epst.question_product_group = ippg.prod_grp_desc(+)
)tgt
where act.cycle_start_date = tgt.cycle_start_date(+)
and   act.cycle_end_date = tgt.cycle_end_date(+)
and   act.acct_key = tgt.acct_id(+)
and   act.prod_grp_key = tgt.prod_grp_key(+)
and   act.todo_desc = tgt.perenso_questions(+)
),
final as (
select
perenso_source::varchar(20) as perenso_source,
todo_type::varchar(20) as todo_type,
store_chk_hdr_key::number(10,0) as store_chk_hdr_key,
store_chk_date::timestamp_ntz(9) as store_chk_date,
line_key::number(10,0) as line_key,
acct_key::number(10,0) as acct_key,
prod_grp_key::number(10,0) as prod_grp_key,
create_user_key::number(10,0) as create_user_key,
todo_desc::varchar(255) as todo_desc,
to_do_start_date::timestamp_ntz(9) as to_do_start_date,
to_do_end_date::timestamp_ntz(9) as to_do_end_date,
to_do_option_desc::varchar(256) as to_do_option_desc,
survery_notes::varchar(100) as survery_notes,
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
cycle_number::varchar(25) as cycle_number,
parent_ques_desc::varchar(256) as parent_ques,
ques_order::number(10,0) as ques_order,
target_type::varchar(25) as target_type,
target::number(10,0) as target,
survey_category::varchar(100) as survey_category
from transformed
)
select * from final