with source as (
    select * from {{ ref('aspedw_integration__edw_material_typ') }}
),
final as (
    select
    matl_type as "matl_type",
langu as "langu",
txtmd as "txtmd",
crt_dttm as "crt_dttm",
updt_dttm as "updt_dttm"
from source
)
select * from final 