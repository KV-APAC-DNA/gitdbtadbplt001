with edw_reg_inventory_health_analysis_propagation as (
    select * from {{ source('aspedw_integration', 'edw_reg_inventory_health_analysis_propagation_sync') }}
),
edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
final as (
    select inv.country_name :: varchar(40) as market,
	cd."cluster" :: varchar(100) as "cluster",	
	month_year :: varchar(23) as month_id,
	'INVENTORY' :: varchar(100) as kpi,
	healthy_inventory_usd :: numeric(38,5) as healthy_inventory_usd,
	total_inventory_val :: numeric(38,5) as total_inventory_val,
	total_inventory_val_usd :: numeric(38,5) as total_inventory_val_usd,
	l12m_weeks_avg_sales_usd :: numeric(38,5) as l12m_weeks_avg_sales_usd
from (
select month_year,
case when country_name like 'China%' then 'China Personal Care' else country_name end as country_name,
sum(case when healthy_inventory = 'Y' and ((country_name <> 'China FTZ' and weeks_cover <= 13) or (country_name = 'China FTZ' and weeks_cover <= 26)) then inventory_val_usd else 0 end) as healthy_inventory_usd,
sum(inventory_val) as total_inventory_val,
sum(inventory_val_usd) as total_inventory_val_usd,
sum(l12m_weeks_avg_sales_usd) as l12m_weeks_avg_sales_usd
from (
	select month_year, country_name, sap_prnt_cust_key, sap_prnt_cust_desc, distributor_id, distributor_id_name, brand, segment, prod_category,
	variant, put_up_description, pka_product_key, healthy_inventory,
	sum(inventory_val) as inventory_val,
	sum(inventory_val_usd) as inventory_val_usd,
	sum(l12m_weeks_avg_sales_usd) as l12m_weeks_avg_sales_usd,
	nvl(sum(inventory_val_usd)/nullif(sum(l12m_weeks_avg_sales_usd), 0),0) as weeks_cover
	from edw_reg_inventory_health_analysis_propagation
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13
	order by 1,2,3,4,5,6,7,8,9,10,11,12,13)
group by 1,2 order by 1,2) inv
left join (select distinct ctry_group, "cluster" from edw_company_dim) cd
on inv.country_name=cd.ctry_group
)

select * from final 