with syouhincd_henkan_qv as (
    select * from {{ ref('jpndcledw_integration__syouhincd_henkan_qv') }}
),

final as (
select itemcode as "itemcode",
	koseiocode as "koseiocode"
from syouhincd_henkan_qv
)

select * from final