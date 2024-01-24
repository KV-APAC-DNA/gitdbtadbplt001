with itg_sg_pos_sales_fact as (
    select * from {{ source('snaposeitg_integration', 'itg_sg_pos_sales_fact') }}
),
itg_mds_ap_customer360_config as (
    select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
--     select * from dev_dna_core.apahil01_workspace.itg_mds_ap_customer360_config
),
base as 
(
--POS
select 'POS' as data_src,
       'SG' as 	cntry_cd,
       'Singapore' as cntry_nm,
       year::int as year,
       mnth_id::int as mnth_id,
	   week::int as week_id,
	   --case when nvl(cast(right(week,2) as varchar(2)),'na')<>'na'
	   --then trunc(to_date(right(week,2)::integer||' '||year::integer,'ww yyyy'))
	   ---(date_part(dow,trunc(to_date(right(week,2)::integer||' --'||year::integer,'ww yyyy'))))::integer+1 
	   --else to_date(mnth_id|| '01','yyyymmdd') end 
	   bill_date as day,
	   sold_to_code as soldto_code,
       sold_to_code as distributor_code,
       cust_id as distributor_name,
       store as store_cd,
       store_name as store_name,
	   store_type as store_type,
	   --'na' as store_type,
       product_barcode as ean,
       sap_code as matl_num,
       product_key as pka_product_key,
       product_key_desc as pka_product_key_description,
	   item_desc as customer_product_desc,
	   'NA' as region,
	   'NA' as zone_or_area,
	   master_code as master_code,
	   sales_qty as so_sls_qty, 
	   net_sales as so_sls_value
      from  itg_sg_pos_sales_fact
      ),
transformed as (
select
base.data_src,
base.cntry_cd,
base.cntry_nm,
base.year,
base.mnth_id,
base.week_id,
base.day,
base.soldto_code,
base.distributor_code,
base.distributor_name,
base.store_cd,
base.store_name,
base.store_type,
base.ean,
base.matl_num,
base.pka_product_key,
base.pka_product_key_description,
base.customer_product_desc,
base.region,
base.zone_or_area,
-----added master code ---
base.master_code,
base.so_sls_qty,
base.so_sls_value,
current_timestamp() as crtd_dttm,
current_timestamp() as updt_dttm
from
base
Where not (nvl(base.so_sls_value, 0) = 0 and nvl(base.so_sls_qty, 0) = 0) and base.day > (select to_date(param_value,'YYYY-MM-DD') from itg_mds_ap_customer360_config where code='min_date') 
and base.mnth_id>= (case when (select param_value from itg_mds_ap_customer360_config where code='base_load_sg')='ALL' then '190001' else to_char(add_months(to_date(current_date::varchar, 'YYYY-MM-DD'), -((select param_value from itg_mds_ap_customer360_config where code='base_load_sg')::integer)), 'YYYYMM')
end)),
final as 
(
SELECT
data_src::varchar(3) as data_src,
cntry_cd::varchar(2) as cntry_cd,
cntry_nm::varchar(9) as cntry_nm,
year::numeric(18,0) as year,
mnth_id::numeric(18,0) as mnth_id,
week_id::numeric(18,0) as week_id,
day::date as day,
soldto_code::varchar(200) as soldto_code,
distributor_code::varchar(200) as distributor_code,
distributor_name::varchar(10) as distributor_name,
store_cd::varchar(300) as store_cd,
store_name::varchar(300) as store_name,
store_type::varchar(200) as store_type,
ean::varchar(255) as ean,
matl_num::varchar(255) as matl_num,
pka_product_key::varchar(300) as pka_product_key,
pka_product_key_description::varchar(500) as pka_product_key_description,
so_sls_qty::numeric(9) as so_sls_qty,
so_sls_value::numeric(9) as so_sls_value,
customer_product_desc::varchar(500) as customer_product_desc,
region::varchar(2) as region,
zone_or_area::varchar(2) as zone_or_area,
-----added master code ---
master_code::varchar(255) as master_code,
current_timestamp() as crtd_dttm,
current_timestamp() as updt_dttm
from transformed
)
select * from final