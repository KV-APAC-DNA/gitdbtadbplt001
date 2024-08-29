with edw_mds_jp_dcl_mt_h_product as (
    select * from {{ ref('jpndcledw_integration__edw_mds_jp_dcl_mt_h_product') }}
),

final as (
select  
"ci-code" as "ci-code",
"happy bag flag" as "happy bag flag",
"outlet flag"  as "outlet flag" ,
"family sale flag" as "family sale flag",
flag01 as "flag01",
flag02 as "flag02",
flag03 as "flag03",
flag04 as "flag04",
flag05 as "flag05",
flag06 as "flag06",
flag07 as "flag07",
flag08 as "flag08",
flag09 as "flag09",
flag10 as "flag10",
description as "description"
from edw_mds_jp_dcl_mt_h_product
)

select * from final