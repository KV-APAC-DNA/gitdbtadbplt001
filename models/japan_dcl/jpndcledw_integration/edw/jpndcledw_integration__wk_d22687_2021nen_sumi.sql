with c_tbecpointadm
as (
    select *
    from {{ ref('jpndclitg_integration__c_tbecpointadm') }}
    ),
kr_comm_point_para
as (
    select *
    from {{ source('jpdcledw_integration', 'kr_comm_point_para') }} 
    ),
c1
as (
    select diecusrid,
        sum(c_diissuepoint) as point
    from c_tbecpointadm
    where 1 = 1
        and dielimflg = '0'
        and divalidflg = '1'
        and diregistdivcode = '20001'
        and to_char(dspointren, 'yyyy') = (
            select target_year
            from kr_comm_point_para
            )
        and (
            dspointmemo like '%シーラボポイントアッププログラム%'
            and to_char(dsprep, 'yyyymmdd') = '20210322'
            )
        or (
            dspointmemo = 'ステージアップボーナスポイント'
            and to_char(dsprep, 'yyyymmdd') <> '20210322'
            )
        or (
            dspointmemo = 'ステージアップボーナスポイント' -- pointmemo desc.
            and to_char(dsprep, 'yyyymmdd') <> '20210322'
            and to_char(dsprep, 'yyyymmdd') <> '20211216' --12.16付与買い回り
            and diregistdivcode = '20005'
            )
    group by diecusrid
    order by diecusrid
    ),
final
as (
    select diecusrid::number(38, 0) as diecusrid,
        point::number(38, 0) as point
    from c1
    )
select *
from final
