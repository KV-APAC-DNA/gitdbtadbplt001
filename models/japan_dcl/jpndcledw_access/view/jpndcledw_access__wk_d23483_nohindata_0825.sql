with wk_d23483_nohindata_0825 as (
    select * from {{ ref('jpndcledw_integration__wk_d23483_nohindata_0825') }}
),

final as (
select  
"受注id",
	"決済id",
	"顧客id",
	"顧客no",
	"受注基準日",
	"基準金額",
	"tsu-han",
	web as "web",
	call as "call",
	store as "store"
from wk_d23483_nohindata_0825
)

select * from final