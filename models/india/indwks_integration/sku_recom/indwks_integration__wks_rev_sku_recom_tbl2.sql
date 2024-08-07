with wks_rev_sku_recom_sm_gtm as
(
    select * from {{ ref('indwks_integration__wks_rev_sku_recom_sm_gtm') }}
),
wks_rev_sku_recom_sm_nongtm as
(
    select * from {{ ref('indwks_integration__wks_rev_sku_recom_sm_nongtm') }}
),
final as
(
SELECT * FROM wks_rev_sku_recom_sm_gtm
UNION ALL
SELECT * FROM wks_rev_sku_recom_sm_nongtm
)
select * from final
