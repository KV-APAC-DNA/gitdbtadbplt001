with item_hanbai_v as (
    select * from {{ ref('jpndcledw_integration__item_hanbai_v') }}
),

final as (
select  
h_itemcode as "h_itemcode",
h_itemname as "h_itemname",
diid as "diid",
tanka_sales as "tanka_sales",
syutoku_kbn as "syutoku_kbn"
from item_hanbai_v
)

select * from final