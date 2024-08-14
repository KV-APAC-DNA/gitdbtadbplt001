with wk_d22687_ruikei
as (
  select *
  from {{ ref('jpndcledw_integration__wk_d22687_ruikei') }}
  ),
wk_d22687_2021nen_sumi
as (
  select *
  from {{ ref('jpndcledw_integration__wk_d22687_2021nen_sumi') }}
  ),
transformed
as (
  select distinct usrid
  from wk_d22687_ruikei
  
  union
  
  select distinct diecusrid
  from wk_d22687_2021nen_sumi
  ),
final
as (
  select usrid::number(38, 0) as usrid
  from transformed
  )
select *
from final
