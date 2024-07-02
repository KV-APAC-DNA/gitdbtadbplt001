with rpt_regional_commercial_excellence_scorecard as (
    select * from {{ ref('aspedw_integration__rpt_regional_commercial_excellence_scorecard') }}
),
final as (
    Select 
    market :: VARCHAR(200) as market       
    ,"cluster" :: VARCHAR(200) as "cluster"       
    ,cust_seg :: VARCHAR(100) as cust_seg          
    ,retail_env :: VARCHAR(100) as retail_env
    ,mega_brand :: VARCHAR(100) as mega_brand     
    ,month_id :: VARCHAR(19) as month_id
	,dso_kv_days :: numeric(18,0) as dso_kv_days
	,ytd_week_passed ::  numeric(18,0) as ytd_week_passed
	,week ::  numeric(18,0) as week
    ,kpi :: VARCHAR(50) as kpi
    ,val_lcy :: DOUBLE PRECISION as val_lcy
	,val_usd :: DOUBLE PRECISION as val_usd
	,mtd_lcy :: DOUBLE PRECISION as mtd_lcy    
    ,mtd_usd :: DOUBLE PRECISION as mtd_usd        
    ,ytd_lcy :: DOUBLE PRECISION as ytd_lcy
    ,ytd_usd :: DOUBLE PRECISION as ytd_usd
	,kv_val_mat :: DOUBLE PRECISION as kv_val_mat
	,kv_val_mat_usd :: DOUBLE PRECISION as kv_val_mat_usd
	,kv_val_ytd :: DOUBLE PRECISION as kv_val_ytd
	,kv_val_ytd_usd :: DOUBLE PRECISION as kv_val_ytd_usd
	,mkt_cat_val_mat :: DOUBLE PRECISION as mkt_cat_val_mat
	,mkt_cat_val_mat_usd :: DOUBLE PRECISION as mkt_cat_val_mat_usd
    ,mkt_cat_val_ytd :: DOUBLE PRECISION as mkt_cat_val_ytd
    ,mkt_cat_val_ytd_usd :: DOUBLE PRECISION as mkt_cat_val_ytd_usd
	,healthy_inventory_usd :: DOUBLE PRECISION as healthy_inventory_usd
	,total_inventory_val :: DOUBLE PRECISION as total_inventory_val
	,total_inventory_val_usd :: DOUBLE PRECISION as total_inventory_val_usd
	,l12m_weeks_avg_sales_usd :: DOUBLE PRECISION as l12m_weeks_avg_sales_usd
	,ytd_dso_gts :: DOUBLE PRECISION as ytd_dso_gts
	,ytd_dso_gross_account_receivable :: DOUBLE PRECISION as ytd_dso_gross_account_receivable
    FROM rpt_regional_commercial_excellence_scorecard
)
select * from final 