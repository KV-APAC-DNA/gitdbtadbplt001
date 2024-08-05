with wks_commercial_excellence_ecomm_base as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_ecomm_base') }}
),
final as (
    Select base.market  :: varchar(40) AS market, 
    base."cluster" :: varchar(100) AS "cluster", 
    base.kpi :: varchar(100) AS kpi, 
    base.month_id :: varchar(23) as month_id, 
    base.mtd_lcy :: numeric(38,5) as mtd_lcy, 
    base.mtd_usd :: numeric(38,5) as mtd_usd,
  Sum(base_ytd.mtd_lcy) :: numeric(38,5) AS ytd_lcy,
  Sum(base_ytd.mtd_usd) :: numeric(38,5) AS ytd_usd
From wks_commercial_excellence_ecomm_base base
   left join wks_commercial_excellence_ecomm_base base_ytd
      on base_ytd.market = base.market AND base_ytd."cluster" = base."cluster" AND base_ytd.kpi = base.kpi      
         and CAST(left(base.month_id,4) as INT) = CAST(left(base_ytd.month_id,4) as INT)        
         and CAST(right(base_ytd.month_id,2) as INT) <= CAST(right(base.month_id,2) as INT) 
Group by base.market, base."cluster", base.kpi, base.month_id, base.mtd_lcy, base.mtd_usd

)
select * from final 