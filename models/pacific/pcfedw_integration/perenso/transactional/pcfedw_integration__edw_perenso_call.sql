with itg_perenso_diary_item_type as (
  select * from {{ ref('pcfitg_integration__itg_perenso_diary_item_type') }}
),
itg_perenso_diary_item as (
  select * from {{ ref('pcfitg_integration__itg_perenso_diary_item') }}
),
itg_perenso_constants as (
  select * from {{ ref('pcfitg_integration__itg_perenso_constants') }}
),
itg_perenso_instore_cycle_dates as (
  select  * from {{ ref('pcfitg_integration__itg_perenso_instore_cycle_dates') }}
),
ipicd as (
    select  TIME, 
        cycle_year,
        to_date((substring(ipicd.start_date,7,4) || '-' ||

                 substring(ipicd.start_date,4,2) || '-' ||

                 substring(ipicd.start_date,1,2)),'YYYY-MM-DD') as start_date,

        to_date((substring(ipicd.end_date,7,4) || '-' ||

                 substring(ipicd.end_date,4,2) || '-' ||

                 substring(ipicd.end_date,1,2)),'YYYY-MM-DD') as end_date
from itg_perenso_instore_cycle_dates ipicd),


transformed as (
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
),
final as (
    select
        perenso_source::varchar(4) as perenso_source,
        diary_item_type_key::number(14,0) as diary_item_type_key,
        diary_item_type_desc::varchar(255) as diary_item_type_desc,
        diary_item_key::number(10,0) as diary_item_key,
        diary_start_time::timestamp_ntz(9) as diary_start_time,
        diary_end_time::timestamp_ntz(9) as diary_end_time,
        acct_key::number(10,0) as acct_key,
        diary_complete::varchar(5) as diary_complete,
        diary_item_complete::number(18,0) as diary_item_complete,
        count_in_call_rate::varchar(5) as count_in_call_rate,
        diary_item_category::varchar(255) as diary_item_category,
        create_user_key::number(10,0) as create_user_key,
        cycle_start_date::date as cycle_start_date,
        cycle_end_date::date as cycle_end_date,
        cycle_number::varchar(20) as cycle_number
    from transformed
)
select * from final