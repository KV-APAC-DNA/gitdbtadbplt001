with itg_th_jbp_market_share_dist as (
select * from {{ ref('thaitg_integration__itg_th_jbp_market_share_dist') }}
),
final as (
select 
  'TH' AS cntry_cd, 
  'Thailand' AS cntry_nm, 
  'Traditional' AS chnl, 
  'Johnson & Johnson' AS mnfctrer, 
  itg_th_jbp_market_share_dist.year_month AS yr_mnth, 
  rtrim(
    ltrim(
      (
        itg_th_jbp_market_share_dist.measure
      ):: text
    )
  ) AS measure, 
  rtrim(
    ltrim(
      (
        itg_th_jbp_market_share_dist.category
      ):: text
    )
  ) AS category, 
  itg_th_jbp_market_share_dist.value 
FROM 
  itg_th_jbp_market_share_dist
)
select * from final 
