with item_hanbai_tbl as (
select * from {{ ref('jpndcledw_integration__item_hanbai_tbl') }}
),
final as (select iht.h_itemcode,
    case 
        when ((iht.h_itemcode)::text = ('X000000002'::character varying)::text)
            then '利用ポイント数(交換)'::CHARACTER VARYING
        ELSE iht.h_itemname
        end as h_itemname,
    iht.diid,
    iht.tanka_sales,
    iht.syutoku_kbn
from item_hanbai_tbl iht)
select * from final