with wks_commercial_excellence_customer_segmentation_summation_ytd as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_customer_segmentation_summation_ytd') }}
),
wks_commercial_excellence_gts_summation_ytd as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_gts_summation_ytd') }}
),
wks_commercial_excellence_gts_phasing_summation as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_gts_phasing_summation') }}
),
wks_commercial_excellence_inventory_base as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_inventory_base') }}
),
wks_commercial_excellence_ecomm_summation_ytd as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_ecomm_summation_ytd') }}
),
wks_commercial_excellence_dso_summation_ytd as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_dso_summation_ytd') }}
),
wks_commercial_excellence_market_share_summation as (
    select * from {{ ref('aspwks_integration__wks_commercial_excellence_market_share_summation') }}
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
    ,val_lcy :: DOUBLE as val_lcy
	,val_usd :: DOUBLE as val_usd
	,mtd_lcy :: DOUBLE as mtd_lcy    
    ,mtd_usd :: DOUBLE as mtd_usd        
    ,ytd_lcy :: DOUBLE as ytd_lcy
    ,ytd_usd :: DOUBLE as ytd_usd
	,kv_val_mat :: DOUBLE as kv_val_mat
	,kv_val_mat_usd :: DOUBLE as kv_val_mat_usd
	,kv_val_ytd :: DOUBLE as kv_val_ytd
	,kv_val_ytd_usd :: DOUBLE as kv_val_ytd_usd
	,mkt_cat_val_mat :: DOUBLE as mkt_cat_val_mat
	,mkt_cat_val_mat_usd :: DOUBLE as mkt_cat_val_mat_usd
    ,mkt_cat_val_ytd :: DOUBLE as mkt_cat_val_ytd
    ,mkt_cat_val_ytd_usd :: DOUBLE as mkt_cat_val_ytd_usd
	,healthy_inventory_usd :: DOUBLE as healthy_inventory_usd
	,total_inventory_val :: DOUBLE as total_inventory_val
	,total_inventory_val_usd :: DOUBLE as total_inventory_val_usd
	,l12m_weeks_avg_sales_usd :: DOUBLE as l12m_weeks_avg_sales_usd
	,ytd_dso_gts :: DOUBLE as ytd_dso_gts
	,ytd_dso_gross_account_receivable :: DOUBLE as ytd_dso_gross_account_receivable
    FROM 
    (
        --START OF MARKET SHARE
        select market        
        ,"cluster"           
        ,'NA' as cust_seg           
        ,'NA' as retail_env 
        ,'NA' as mega_brand       
        ,month_id 
        ,0 as dso_kv_days
        ,0 as ytd_week_passed
        ,0 as week
        ,kpi 
        ,0 as val_lcy
        ,0 as val_usd
        ,0 as mtd_lcy       
        ,0 as mtd_usd         
        ,0 as ytd_lcy 
        ,0 as ytd_usd
        ,kv_val_mat
        ,kv_val_mat_usd 
        ,kv_val_ytd
        ,kv_val_ytd_usd 
        ,mkt_cat_val_mat
        ,mkt_cat_val_mat_usd
        ,mkt_cat_val_ytd 
        ,mkt_cat_val_ytd_usd
        ,0 as healthy_inventory_usd
        ,0 as total_inventory_val
        ,0 as total_inventory_val_usd
        ,0 as l12m_weeks_avg_sales_usd
        ,0 as ytd_dso_gts
        ,0 as ytd_dso_gross_account_receivable
        from wks_commercial_excellence_market_share_summation
        where left(month_id,4)>=(select extract(year from CURRENT_TIMESTAMP())-2)
        --END OF MARKET SHARE
        UNION ALL
        --START OF ALL NTS
        select market        
        ,"cluster"           
        ,cust_seg           
        ,retail_env 
        ,mega_brand       
        ,month_id  
        ,0 as dso_kv_days
        ,ytd_week_passed
        ,0 as week
        ,kpi 
        ,0 as val_lcy
        ,0 as val_usd
        ,mtd_lcy       
        ,mtd_usd         
        ,ytd_lcy 
        ,ytd_usd
        ,0 as kv_val_mat
        ,0 as kv_val_mat_usd 
        ,0 as kv_val_ytd
        ,0 as kv_val_ytd_usd 
        ,0 as mkt_cat_val_mat
        ,0 as mkt_cat_val_mat_usd
        ,0 as mkt_cat_val_ytd 
        ,0 as mkt_cat_val_ytd_usd
        ,0 as healthy_inventory_usd
        ,0 as total_inventory_val
        ,0 as total_inventory_val_usd
        ,0 as l12m_weeks_avg_sales_usd
        ,0 as ytd_dso_gts
        ,0 as ytd_dso_gross_account_receivable
        from wks_commercial_excellence_customer_segmentation_summation_ytd
        where left(month_id,4)>=(select extract(year from CURRENT_TIMESTAMP())-2)
        --END OF ALL NTS
        UNION ALL
        --START OF ALL GTS
        select market        
        ,"cluster"           
        ,cust_seg           
        ,'NA' as retail_env 
        ,mega_brand       
        ,month_id  
        ,0 as dso_kv_days
        ,0 as ytd_week_passed
        ,0 as week
        ,kpi 
        ,0 as val_lcy
        ,0 as val_usd
        ,mtd_lcy       
        ,mtd_usd         
        ,ytd_lcy 
        ,ytd_usd
        ,0 as kv_val_mat
        ,0 as kv_val_mat_usd 
        ,0 as kv_val_ytd
        ,0 as kv_val_ytd_usd 
        ,0 as mkt_cat_val_mat
        ,0 as mkt_cat_val_mat_usd
        ,0 as mkt_cat_val_ytd 
        ,0 as mkt_cat_val_ytd_usd
        ,0 as healthy_inventory_usd
        ,0 as total_inventory_val
        ,0 as total_inventory_val_usd
        ,0 as l12m_weeks_avg_sales_usd
        ,0 as ytd_dso_gts
        ,0 as ytd_dso_gross_account_receivable
        from wks_commercial_excellence_gts_summation_ytd
        where left(month_id,4)>=(select extract(year from CURRENT_TIMESTAMP())-2)
        --END OF ALL GTS
        UNION ALL
        --START OF GTS PHASING
        select market        
        ,"cluster"           
        ,'NA' as cust_seg           
        ,'NA' as retail_env 
        ,mega_brand       
        ,month_id  
        ,0 as dso_kv_days
        ,0 as ytd_week_passed
        ,week
        ,kpi 
        ,val_lcy
        ,val_usd
        ,0 as mtd_lcy       
        ,0 as mtd_usd         
        ,0 as ytd_lcy 
        ,0 as ytd_usd
        ,0 as kv_val_mat
        ,0 as kv_val_mat_usd 
        ,0 as kv_val_ytd
        ,0 as kv_val_ytd_usd 
        ,0 as mkt_cat_val_mat
        ,0 as mkt_cat_val_mat_usd
        ,0 as mkt_cat_val_ytd 
        ,0 as mkt_cat_val_ytd_usd
        ,0 as healthy_inventory_usd
        ,0 as total_inventory_val
        ,0 as total_inventory_val_usd
        ,0 as l12m_weeks_avg_sales_usd
        ,0 as ytd_dso_gts
        ,0 as ytd_dso_gross_account_receivable
        from wks_commercial_excellence_gts_phasing_summation
        where left(month_id,4)>=(select extract(year from CURRENT_TIMESTAMP())-2)
        --END OF GTS PHASING
        UNION ALL
        --START OF INVENTORY
        select market        
        ,"cluster"           
        ,'NA' as cust_seg           
        ,'NA' as retail_env 
        ,'NA' as mega_brand       
        ,month_id  
        ,0 as dso_kv_days
        ,0 as ytd_week_passed
        ,0 as week
        ,kpi 
        ,0 as val_lcy
        ,0 as val_usd
        ,0 as mtd_lcy       
        ,0 as mtd_usd         
        ,0 as ytd_lcy 
        ,0 as ytd_usd
        ,0 as kv_val_mat
        ,0 as kv_val_mat_usd 
        ,0 as kv_val_ytd
        ,0 as kv_val_ytd_usd 
        ,0 as mkt_cat_val_mat
        ,0 as mkt_cat_val_mat_usd
        ,0 as mkt_cat_val_ytd 
        ,0 as mkt_cat_val_ytd_usd
        ,healthy_inventory_usd
        ,total_inventory_val
        ,total_inventory_val_usd
        ,l12m_weeks_avg_sales_usd
        ,0 as ytd_dso_gts
        ,0 as ytd_dso_gross_account_receivable
        from wks_commercial_excellence_inventory_base
        where left(month_id,4)>=(select extract(year from CURRENT_TIMESTAMP())-2)
        --END OF INVENTORY
        UNION ALL
        --START OF ECOM NTS
        select market        
        ,"cluster"           
        ,'NA' as cust_seg           
        ,'NA' as retail_env 
        ,'NA' as mega_brand       
        ,month_id  
        ,0 as dso_kv_days
        ,0 as ytd_week_passed
        ,0 as week
        ,kpi 
        ,0 as val_lcy
        ,0 as val_usd
        ,mtd_lcy       
        ,mtd_usd         
        ,ytd_lcy 
        ,ytd_usd
        ,0 as kv_val_mat
        ,0 as kv_val_mat_usd 
        ,0 as kv_val_ytd
        ,0 as kv_val_ytd_usd 
        ,0 as mkt_cat_val_mat
        ,0 as mkt_cat_val_mat_usd
        ,0 as mkt_cat_val_ytd 
        ,0 as mkt_cat_val_ytd_usd
        ,0 as healthy_inventory_usd
        ,0 as total_inventory_val
        ,0 as total_inventory_val_usd
        ,0 as l12m_weeks_avg_sales_usd
        ,0 as ytd_dso_gts
        ,0 as ytd_dso_gross_account_receivable
        from wks_commercial_excellence_ecomm_summation_ytd
        where left(month_id,4)>=(select extract(year from CURRENT_TIMESTAMP())-2)
        --END OF ECOM NTS
        UNION ALL
        --START OF DSO
        select market        
        ,"cluster"           
        ,'NA' as cust_seg           
        ,'NA' as retail_env 
        ,'NA' as mega_brand       
        ,month_id  
        ,dso_jnj_days as dso_kv_days
        ,0 as ytd_week_passed
        ,0 as week
        ,kpi 
        ,0 as val_lcy
        ,0 as val_usd
        ,0 as mtd_lcy       
        ,0 as mtd_usd         
        ,0 as ytd_lcy 
        ,0 as ytd_usd
        ,0 as kv_val_mat
        ,0 as kv_val_mat_usd 
        ,0 as kv_val_ytd
        ,0 as kv_val_ytd_usd 
        ,0 as mkt_cat_val_mat
        ,0 as mkt_cat_val_mat_usd
        ,0 as mkt_cat_val_ytd 
        ,0 as mkt_cat_val_ytd_usd
        ,0 as healthy_inventory_usd
        ,0 as total_inventory_val
        ,0 as total_inventory_val_usd
        ,0 as l12m_weeks_avg_sales_usd
        ,ytd_dso_gts
        ,ytd_dso_gross_account_receivable
        from wks_commercial_excellence_dso_summation_ytd
        where left(month_id,4)>=(select extract(year from CURRENT_TIMESTAMP())-2)
        --END OF DSO
    )
)
select * from final 