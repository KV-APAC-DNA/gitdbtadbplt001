with wks_commercial_excellence_market_share_base as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_market_share_base') }}
),
final as (
    SELECT 
  base.market :: varchar(40) as market, 
  base."cluster" :: varchar(100) as "cluster", 
  base.month_id :: varchar(23) as month_id, 
  base.kpi :: varchar(100) as kpi,
  sum(base_ytd.kv_val_lc) :: numeric(38,5) as kv_val_ytd,
  sum(base_ytd.kv_val_usd) :: numeric(38,5) as kv_val_ytd_usd,
  sum(base_ytd.cat_val_lc) :: numeric(38,5) as mkt_cat_val_ytd,
  sum(base_ytd.cat_val_usd) :: numeric(38,5) as mkt_cat_val_ytd_usd
From wks_commercial_excellence_market_share_base base
   left join wks_commercial_excellence_market_share_base base_ytd
      ON base_ytd.market = base.market and base_ytd."cluster" = base."cluster" and base_ytd.kpi = base.kpi
         and cast(left(base.month_id,4) as int) = cast(left(base_ytd.month_id,4) as int)
         and cast(right(base_ytd.month_id,2) as int) <= cast(right(base.month_id,2) as int)
group by base.market, base."cluster", base.month_id, base.kpi

)
select * from final 