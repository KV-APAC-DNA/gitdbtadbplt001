with kr_comm_point_para as (
    select * from {{ source('jpdcledw_integration', 'kr_comm_point_para') }}
),

kr_054_nouhinwk1x as (
    select * from {{ ref('jpndcledw_integration__kr_054_nouhinwk1x') }}
),

tbusrpram as (
    select * from {{ ref('jpndclitg_integration__tbusrpram') }}
),

result as (
    select kr054.kokyano as diecusrid,
    kcpp.bd_target_year || substring(usr.dsbirthday, 6, 2) as point_ym,
    kcpp.bd_target_year as point_yy,
    substring(usr.dsbirthday, 6, 2) as point_mm,
    kr054.fuyozumi_point as fuyozumi_point,
    kr054.fuyoyotei_point as fuyoyotei_point,
    kr054.konyu_kingaku as konyu_kingaku
from kr_comm_point_para kcpp,
    kr_054_nouhinwk1x kr054
inner join tbusrpram usr on kr054.kokyano = usr.diusrid
where usr.dsdat12 <> 'ブラック'
    and usr.dielimflg = '0'
    and usr.dsbirthday not like '1600%'
    and usr.disecessionflg = '0'
    and usr.dsdat93 = '通常ユーザ'
    ),

final as (
    select
        diecusrid::number(38,0) as diecusrid,
        point_ym::varchar(6) as point_ym,
        point_yy::varchar(4) as point_yy,
        point_mm::varchar(2) as point_mm,
        fuyozumi_point::number(18,0) as fuyozumi_point,
        fuyoyotei_point::number(18,0) as fuyoyotei_point,
        konyu_kingaku::number(18,0) as konyu_kingaku
    from result
)

select * from final