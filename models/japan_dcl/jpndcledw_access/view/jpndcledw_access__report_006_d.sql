with report_006_d as (
    select * from {{ ref('jpndcledw_integration__report_006_d') }}
),

final as(
select "契約日付",
    nextid as "nextid",
    opecode as "opecode",
    opename as "opename",
    "定期契約顧客no.",
    item_id as "item_id",
    item_name as "item_name",
    cnt_item as "cnt_item",
    total_revenue as "total_revenue",
    diitemsalescost as "diitemsalescost",
    contract_kubun as "contract_kubun",
    order_id as "order_id",
    kessai_id as "kessai_id",
    userid as "userid",
    logincode as "logincode"
from report_006_d
)

select * from final