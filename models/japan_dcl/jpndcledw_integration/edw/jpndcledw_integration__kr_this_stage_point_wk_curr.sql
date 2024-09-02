with c_tbecranksumamount
as (
    select *
    from  {{ ref('jpndclitg_integration__c_tbecranksumamount') }}
    ),
c_tbecrankaddamountadm
as (
    select *
    from {{ ref('jpndclitg_integration__c_tbecrankaddamountadm') }}
    ),
tbusrpram
as (
    select *
    from {{ ref('jpndclitg_integration__tbusrpram') }}
    ),
kr_this_stage_point_wk_rescue
as (
    select *
    from  {{ ref('jpndcledw_integration__kr_this_stage_point_wk_rescue') }}
    ),
dcl_calendar_sysdate
as (
    select *
    from {{ ref('jpndcledw_integration__dcl_calendar_sysdate') }}
    ),
sum_curr_month
as (
    select amt.diecusrid as usrid,
        amt.c_dsaggregateym as rankdt,
        amt.c_dsranktotalprcbymonth as prc
    from c_tbecranksumamount amt
    where amt.dielimflg = '0'
    
    union all
    
    select diecusrid as usrid,
        to_char(dsorderdt, 'yyyymm') as rankdt,
        c_dsrankaddprc as prc
    from c_tbecrankaddamountadm adda
    where dielimflg = '0'
    
    union all
    
    select usrid as usrid,
        yyyymm as rankdt,
        diff_amout as prc
    from kr_this_stage_point_wk_rescue
    ),
ruikei
as (
    select 
        to_char(cal.curr_date, 'yyyymm') as yyyymm,
        sum_curr_month.usrid as usrid,
        sum(sum_curr_month.prc) as thistotalprc
    from 
        sum_curr_month
    join 
        dcl_calendar_sysdate cal 
        on cal.is_active = true
        and 1 = 1
    where 
        sum_curr_month.rankdt between to_char(cal.curr_date, 'yyyy') || '01'
        and to_char(cal.curr_date, 'yyyymm')
    group by to_char(cal.curr_date, 'yyyymm'),
        sum_curr_month.usrid
    ),
transformed
as (
    select ruikei.yyyymm,
        ruikei.usrid,
        case 
            when ruikei.thistotalprc >= 80000
                then 'ダイヤモンド'
            when ruikei.thistotalprc >= 50000
                then 'プラチナ'
            when ruikei.thistotalprc >= 15000
                then 'ゴールド'
            else 'レギュラー'
            end as tstage,
        case 
            when ruikei.thistotalprc >= 80000
                then '04'
            when ruikei.thistotalprc >= 50000
                then '03'
            when ruikei.thistotalprc >= 15000
                then '02'
            else '01'
            end as tstage_cd,
        nvl(ruikei.thistotalprc, 0) as thistotalprc,
        case 
            when ruikei.thistotalprc >= 80000
                then 8500
            when ruikei.thistotalprc >= 50000
                then 3500
            when ruikei.thistotalprc >= 15000
                then 500
            else 0
            end as goalp,
        to_char(convert_timezone('UTC', 'Asia/Tokyo', current_timestamp()), 'yyyymmdd hh24:mi:ss') insertdate
    from ruikei
    where ruikei.usrid in (
            select diusrid
            from tbusrpram usr
            where usr.disecessionflg = 0
            )
    ),
final as
(
    select 
        yyyymm::varchar(14) as yyyymm,
        usrid::number(38,0) as usrid,
        tstage::varchar(18) as tstage,
        tstage_cd::varchar(2) as tstage_cd,
        thistotalprc::number(38,0) as thistotalprc,
        goalp::number(18,0) as goalp,
        insertdate::varchar(27) as insertdate
    from transformed
)

select *
from final