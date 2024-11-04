WITH 
SDL_TW_POS_AMART_INVENTORY as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_amart_inventory') }}
),
SDL_TW_POS_AMART_PURCHASE as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_amart_purchase') }}
),
inventory AS (
    SELECT mnth_id::numeric as mnth_id,
           item_code,
           item_desc,
           customer_code,
           SUM(inventory_qty) as inventory_qty
    FROM SDL_TW_POS_AMART_INVENTORY
    GROUP BY 1,2,3,4
),
purchases AS (
    SELECT product_code,
           TO_CHAR(purchase_date::DATE, 'YYYYMM')::numeric AS mnth_id,
           SUM(purchase_qty) as purchase_qty
    FROM SDL_TW_POS_AMART_PURCHASE
    GROUP BY 1,2
),

final as (
SELECT 
    curr.mnth_id,
    curr.item_code,
    curr.item_desc,
    curr.customer_code,
    curr.inventory_qty,
    p.purchase_qty,
    (prev.inventory_qty + COALESCE(p.purchase_qty,0) - curr.inventory_qty) as offtake_qty
FROM inventory curr
LEFT JOIN inventory prev ON curr.item_code = prev.item_code 
    AND curr.mnth_id = prev.mnth_id + 1
LEFT JOIN purchases p ON curr.item_code = p.product_code 
    AND curr.mnth_id = p.mnth_id)

select * from final    