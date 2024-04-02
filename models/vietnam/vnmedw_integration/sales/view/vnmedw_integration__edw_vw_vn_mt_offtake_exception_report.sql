with edw_vw_vn_mt_pos_offtake as 
(
    select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_pos_offtake') }}
),
final as 
(
select distinct edw_vw_vn_mt_pos_offtake.account,
    edw_vw_vn_mt_pos_offtake.month_id,
    edw_vw_vn_mt_pos_offtake.product_cd,
    edw_vw_vn_mt_pos_offtake.customer_cd,
    edw_vw_vn_mt_pos_offtake.store_name,
    edw_vw_vn_mt_pos_offtake.barcode,
    edw_vw_vn_mt_pos_offtake.product_name,
    edw_vw_vn_mt_pos_offtake.franchise,
    edw_vw_vn_mt_pos_offtake.category,
    edw_vw_vn_mt_pos_offtake.sub_brand,
    edw_vw_vn_mt_pos_offtake.sub_category,
    edw_vw_vn_mt_pos_offtake.amount,
    edw_vw_vn_mt_pos_offtake.amount_usd
from edw_vw_vn_mt_pos_offtake
where (
        (
            (
                (
                    (
                        (
                            (edw_vw_vn_mt_pos_offtake.product_name is null)
                            or (edw_vw_vn_mt_pos_offtake.account is null)
                        )
                        or (edw_vw_vn_mt_pos_offtake.franchise is null)
                    )
                    or (edw_vw_vn_mt_pos_offtake.category is null)
                )
                or (edw_vw_vn_mt_pos_offtake.sub_brand is null)
            )
            or (edw_vw_vn_mt_pos_offtake.sub_category is null)
        )
        or (edw_vw_vn_mt_pos_offtake.store_name is null)
)
)

select * from final