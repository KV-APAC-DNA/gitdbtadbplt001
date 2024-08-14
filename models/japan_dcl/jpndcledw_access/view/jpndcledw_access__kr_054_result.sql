with kr_054_result as (
    select * from {{ ref('jpndcledw_integration__kr_054_result') }}
),

final as (
select  
no as "no",
lg_item as "lg_item",
md_item as "md_item",
sm_kb as "sm_kb",
sm_nm as "sm_nm",
pt_sum as "pt_sum",
pt_pdt as "pt_pdt",
pt_rsn as "pt_rsn"
from kr_054_result
)

select * from final