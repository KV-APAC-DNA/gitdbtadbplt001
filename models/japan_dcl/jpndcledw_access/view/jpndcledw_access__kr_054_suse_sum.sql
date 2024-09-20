with kr_054_suse_sum as (
    select * from {{ ref('jpndcledw_integration__kr_054_suse_sum') }}
),

final as (
select  
use_label as "use_label",
use_ym as "use_ym",
use_yy as "use_yy",
use_mm as "use_mm",
point_ym as "point_ym",
point_yy as "point_yy",
point_mm as "point_mm",
upoint as "upoint"
from kr_054_suse_sum
)

select * from final