with source as (
    select * from {{ ref('aspedw_integration__edw_material_uom') }}
),
final as (
    select
    material as "material",
unit as "unit",
base_uom as "base_uom",
record_mode as "record_mode",
uomz1d as "uomz1d",
uomn1d as "uomn1d",
cdl_dttm as "cdl_dttm",
crtd_dttm as "crtd_dttm",
updt_dttm as "updt_dttm"
from source
)
select * from final 