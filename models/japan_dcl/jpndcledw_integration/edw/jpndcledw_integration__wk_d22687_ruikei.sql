{% if build_month_end_job_models()  %}
with c_tbecranksumamount
as (
    select *
    from {{ ref('jpndclitg_integration__c_tbecranksumamount') }}
    ),
tbusrpram
as (
    select *
    from {{ ref('jpndclitg_integration__tbusrpram') }}
    ),
kr_comm_point_para
as (
    select *
    from {{ source('jpdcledw_integration', 'kr_comm_point_para') }} 
    ),
a
as (
    select diecusrid as usrid,
        c_dsaggregateym as rankdt,
        c_dsranktotalprcbymonth as prc
    from c_tbecranksumamount
    where dielimflg = '0'
        and
        --modification for correction 20210114 start
        --substring(c_dsaggregateym,1,6) between (select cast(extract(year from sysdate) as varchar) +'01') and (select substring(term_end,1,6) from kr_comm_point_para )) a
        substring(c_dsaggregateym, 1, 6) between (
                    select target_year || '01'
                    from kr_comm_point_para
                    )
            and (
                    select substring(term_end, 1, 6)
                    from kr_comm_point_para
                    )
    ),
c1
as (
    select a.usrid,
        a.rankdt,
        sum(a.prc) as price
    from a
    --modification for correction 20210114 end
    where exists (
            select 'X'
            from tbusrpram b
            where b.dielimflg = '0'
                and a.usrid = b.diusrid
                and b.disecessionflg = '0'
                and b.dsdat93 = '通常ユーザ'
            )
    group by a.usrid,
        a.rankdt
    having sum(a.prc) >= 1
    ),
final
as (
    select usrid::number(38, 0) as usrid,
        rankdt::varchar(9) as rankdt,
        price::number(38, 0) as price
    from c1
    )
select *
from final
{% else %}
    select * from {{this}}
{% endif %}