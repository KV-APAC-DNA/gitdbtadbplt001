with rpt_regional_commercial_excellence_scorecard as (
    select * from {{ ref('aspedw_integration__rpt_regional_commercial_excellence_scorecard') }}
),
final as (
    Select 
    market :: VARCHAR(200) as "market"      
    ,"cluster" :: VARCHAR(200) as "cluster"       
    ,cust_seg :: VARCHAR(100) as "cust_seg"          
    ,retail_env :: VARCHAR(100) as "retail_env"
    ,mega_brand :: VARCHAR(100) as "mega_brand"    
    ,month_id :: VARCHAR(19) as "month_id"
	,dso_kv_days :: numeric(18,0) as "dso_kv_days"
	,ytd_week_passed ::  numeric(18,0) as "ytd_week_passed"
	,week ::  numeric(18,0) as "week"
    ,kpi :: VARCHAR(50) as "kpi"
    ,val_lcy :: DOUBLE as "val_lcy"
	,val_usd :: DOUBLE as "val_usd"
	,mtd_lcy :: DOUBLE as "mtd_lcy"    
    ,mtd_usd :: DOUBLE as "mtd_usd"        
    ,ytd_lcy :: DOUBLE as "ytd_lcy"
    ,ytd_usd :: DOUBLE as "ytd_usd"
	,kv_val_mat :: DOUBLE as "kv_val_mat"
	,kv_val_mat_usd :: DOUBLE as "kv_val_mat_usd"
	,kv_val_ytd :: DOUBLE as "kv_val_ytd"
	,kv_val_ytd_usd :: DOUBLE as "kv_val_ytd_usd"
	,mkt_cat_val_mat :: DOUBLE as "mkt_cat_val_mat"
	,mkt_cat_val_mat_usd :: DOUBLE as "mkt_cat_val_mat_usd"
    ,mkt_cat_val_ytd :: DOUBLE as "mkt_cat_val_ytd"
    ,mkt_cat_val_ytd_usd :: DOUBLE as "mkt_cat_val_ytd_usd"
	,healthy_inventory_usd :: DOUBLE as "healthy_inventory_usd"
	,total_inventory_val :: DOUBLE as "total_inventory_val"
	,total_inventory_val_usd :: DOUBLE as "total_inventory_val_usd"
	,l12m_weeks_avg_sales_usd :: DOUBLE as "l12m_weeks_avg_sales_usd"
	,ytd_dso_gts :: DOUBLE as "ytd_dso_gts"
	,ytd_dso_gross_account_receivable :: DOUBLE as "ytd_dso_gross_account_receivable"
    FROM rpt_regional_commercial_excellence_scorecard
)
select * from final 