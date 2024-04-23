with source as (
    select * from {{ ref('aspedw_integration__edw_prod_hier') }}
),
final as (
    select
    prod_hier as "prod_hier",
langu as "langu",
txtmd as "txtmd",
crt_dttm as "crt_dttm",
updt_dttm as "updt_dttm"
from source
)
select * from final 