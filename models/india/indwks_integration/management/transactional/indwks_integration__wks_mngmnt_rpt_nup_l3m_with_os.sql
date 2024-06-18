with wks_mngmnt_rpt_nup_l3m as
(
    select * from {{ ref('indwks_integration__wks_mngmnt_rpt_nup_l3m') }}
),
wks_mgmnt_orange_stores as
(
    select * from {{ ref('indwks_integration__wks_mgmnt_orange_stores') }}
),
final as
(
    SELECT nup.*, COALESCE(os."os_y/n_flag",'NO') AS os_flag
    FROM wks_mngmnt_rpt_nup_l3m nup
    LEFT JOIN wks_mgmnt_orange_stores os
       ON nup.mth_mm = os.mth_mm
      AND nup.customer_code = os.cust_cd
      AND nup.retailer_code = os.retailer_cd
)
select * from final