with tm13item_qv as (
    select * from {{ ref('jpndcledw_integration__tm13item_qv') }}
),

final as (
select
itemcode as "itemcode",
itemname as "itemname",
itemnamer as "itemnamer",
itemkbn as "itemkbn",
bunruicode2 as "bunruicode2",
bunruicode3 as "bunruicode3",
bunruicode5 as "bunruicode5",
settanpinkbncode as "settanpinkbncode",
settanpinsetkbn as "settanpinsetkbn",
teikikeiyaku as "teikikeiyaku"
from tm13item_qv
)

select * from final