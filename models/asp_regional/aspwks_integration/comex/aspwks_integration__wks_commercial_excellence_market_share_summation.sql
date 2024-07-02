with wks_commercial_excellence_market_share_base as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_market_share_base') }}
),
wks_commercial_excellence_market_share_summation_ytd as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_market_share_summation_ytd') }}
),
final as (
    select 
  base.market :: varchar(40) as market, 
  base."cluster" :: varchar(100) as "cluster", 
  base.month_id :: varchar(23) as month_id, 
  base.kpi :: varchar(100) as kpi, 
  base.kv_val_ytd :: numeric(38,5) as kv_val_ytd, 
  base.kv_val_ytd_usd :: numeric(38,5) as kv_val_ytd_usd, 
  base.mkt_cat_val_ytd :: numeric(38,5) as mkt_cat_val_ytd, 
  base.mkt_cat_val_ytd_usd :: numeric(38,5) as mkt_cat_val_ytd_usd,
  sum(base_mat.kv_val_lc) :: numeric(38,5) as kv_val_mat,
  sum(base_mat.kv_val_usd) :: numeric(38,5) as kv_val_mat_usd,
  sum(base_mat.cat_val_lc) :: numeric(38,5) as mkt_cat_val_mat,
  sum(base_mat.cat_val_usd) :: numeric(38,5) as mkt_cat_val_mat_usd
From wks_commercial_excellence_market_share_summation_ytd base
   left join wks_commercial_excellence_market_share_base base_mat
      on base_mat.market = base.market and base_mat."cluster" = base."cluster" and base_mat.kpi = base.kpi
         and ((cast(left(base.month_id,4) as int) = cast(left(base_mat.month_id,4) as int)
          and cast(right(base_mat.month_id,2) as int) <= cast(right(base.month_id,2) as int))
         or (cast(left(base.month_id,4) as int) = (cast(left(base_mat.month_id,4) as int)+1)
          and cast(right(base_mat.month_id,2) as int) > cast(right(base.month_id,2) as int)))
group by base.market, base."cluster", base.month_id, base.kpi, base.kv_val_ytd, base.kv_val_ytd_usd, base.mkt_cat_val_ytd, base.mkt_cat_val_ytd_usd	

)
select * from final 