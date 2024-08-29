with dm_kesai_mart_rakuten as (
    select * from {{ ref('jpndcledw_integration__dm_kesai_mart_rakuten') }}
),

final as (
select  
kokyano as "kokyano",
saleno_key as "saleno_key",
saleno as "saleno",
order_dt as "order_dt",
channel as "channel",
juchkbn as "juchkbn",
h_o_item_code as "h_o_item_code",
h_o_item_name as "h_o_item_name",
h_o_item_cname as "h_o_item_cname",
h_o_item_anbun_qty as "h_o_item_anbun_qty",
h_item_code as "h_item_code",
z_item_code as "z_item_code",
z_item_suryo as "z_item_suryo",
gts as "gts",
gts_qty as "gts_qty",
ciw_discount as "ciw_discount",
ciw_point as "ciw_point",
ciw_return as "ciw_return",
ciw_return_qty as "ciw_return_qty",
nts as "nts",
inserted_date as "inserted_date",
inserted_by as "inserted_by",
updated_date as "updated_date",
updated_by as "updated_by"
from dm_kesai_mart_rakuten
)

select * from final