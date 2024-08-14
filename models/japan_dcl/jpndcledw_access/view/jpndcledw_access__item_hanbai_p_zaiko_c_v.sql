with item_hanbai_p_zaiko_c_v as (
    select * from {{ ref('jpndcledw_integration__item_hanbai_p_zaiko_c_v') }}
),

final as (
select  
h_o_item_code as "h_o_item_code",
z_item_code as "z_item_code"
from item_hanbai_p_zaiko_c_v
)

select * from final