
with source as (
    select * from {{ source('jpdcledw_integration', '"CEWQ028破損商品"') }}
),

final as (
select
    "返品no",
    "返品明細no",
    "返品日",
    "商品",
    "商品名",
    "数量",
    "単価",
    "金額",
    "運送会社コード",
    "キャンセルフラグ",
    "削除フラグ",
    "返品区分",
    "抽出日付",
    "受注no",
    "送り状no"
from source
)

select * from final