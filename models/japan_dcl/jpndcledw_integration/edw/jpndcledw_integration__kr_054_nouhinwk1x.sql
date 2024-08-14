with kr_054_tounen_sumix as (
    select * from  {{ ref('jpndcledw_integration__kr_054_tounen_sumix') }}
),

kr_054_tounen_yoteix as (
    select * from {{ ref('jpndcledw_integration__kr_054_tounen_yoteix') }}
),

kr_054_tounen_kingaku_wk2x as (
    select * from {{ ref('jpndcledw_integration__kr_054_tounen_kingaku_wk2x') }}
),

kr_054_user_allx as (
    select * from {{ ref('jpndcledw_integration__kr_054_user_allx') }}
),

result as (

select u.kokyano as kokyano,
    s.point as fuyozumi_point,
    y.point as fuyoyotei_point,
    k.totalprc as konyu_kingaku
from kr_054_user_allx u
left join kr_054_tounen_sumix s on u.kokyano = s.kokyano
left join kr_054_tounen_yoteix y on u.kokyano = y.kokyano
left join kr_054_tounen_kingaku_wk2x k on u.kokyano = k.usrid
),

final as (
select  
    kokyano::number(38,18) as kokyano,
	fuyozumi_point::number(38,18) as fuyozumi_point,
	fuyoyotei_point::number(38,18) as fuyoyotei_point,
	konyu_kingaku::number(38,18) as konyu_kingaku
from result
)

select * from final