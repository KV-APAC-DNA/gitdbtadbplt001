with kr_054_tounen_sumix as (
    select * from  {{ ref('jpndcledw_integration__kr_054_tounen_sumix') }}
),

kr_054_tounen_yoteix as (
    select * from {{ ref('jpndcledw_integration__kr_054_tounen_yoteix') }}
),

kr_054_tounen_kingaku_wk2x as (
    select * from  {{ ref('jpndcledw_integration__kr_054_tounen_kingaku_wk2x') }}
),

result as (
select kokyano from kr_054_tounen_sumix
 union
select kokyano from kr_054_tounen_yoteix
 union
select usrid from kr_054_tounen_kingaku_wk2x
)

select
    kokyano::number(38,18) as kokyano
from result