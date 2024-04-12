-- ALTER TABLE AU_EDW.EDW_PERENSO_CALL

-- ADD COLUMN CYCLE_START_DATE DATE;

-- 

-- ALTER TABLE AU_EDW.EDW_PERENSO_CALL

-- ADD COLUMN CYCLE_END_DATE DATE;



-- ALTER TABLE AU_EDW.EDW_PERENSO_CALL

-- ADD COLUMN CYCLE_NUMBER VARCHAR(20);
with itg_perenso_diary_item_type as (
  select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_DIARY_ITEM_TYPE
),
itg_perenso_diary_item as (
  select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.ITG_PERENSO_DIARY_ITEM
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


final as (
select 'Call' as perenso_source,

       ipdit.diary_item_type_key,

       ipdit.diary_item_type_desc,

       ipdi.diary_item_key,

       ipdi.start_time as diary_start_time,

       ipdi.end_time as diary_end_time,

       ipdi.acct_key,

       ipdi.complete as diary_complete,

       case

         when upper(ipdi.complete) = 'TRUE' then 1

         else 0

       end as diary_item_complete,

       ipdit.count_in_call_rate,

       ipc.const_desc as diary_item_category,

       ipdi.create_user_key,

       ipicd.start_date as cycle_start_date,

       ipicd.end_date as cycle_end_date,

       ipicd.time as cycle_number

from itg_perenso_diary_item_type ipdit,

     itg_perenso_diary_item ipdi,

     itg_perenso_constants ipc,

      ipicd

where ipdi.diary_item_type_key = ipdit.diary_item_type_key (+)

--and   upper(ipdit.active) = 'TRUE'

and   trim(ipdit.category) = trim(ipc.const_key)

and   ipdi.start_time::date >= ipicd.start_date(+)

and   ipdi.start_time::date <= ipicd.end_date(+)
)
select * from final




