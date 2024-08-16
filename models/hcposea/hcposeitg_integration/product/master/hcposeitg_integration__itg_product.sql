{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}}
where (product_source_id) in (select product_source_id
                              from dev_dna_load.hcposesdl_raw.sdl_hcp_osea_product stg_prod
                              where stg_prod.product_source_id = product_source_id)
and   country_code = (select distinct country_code from dev_dna_load.hcposesdl_raw.sdl_hcp_osea_product);
                    {% endif %}"
    )
}}

with sdl_hcp_osea_product as
(
    select * from dev_dna_load.hcposesdl_raw.sdl_hcp_osea_product
)
,
transformed
as
(
select md5(country||product_source_id) as product_indication_key,
       md5(country||parent_product_source_id) as parent_product_key,
       product_source_id,
       owner_source_id,
       (case when upper(is_deleted) = 'true' then 1 when upper(is_deleted) = 'false' then 0 else 0 end) as is_deleted,
       product_name,
       record_type_source_id,
       created_date,
       created_by_id,
       last_modified_date,
       last_modified_by_id,
       consumer_site,
       product_info,
       therapeutic_class,
       parent_product_source_id,
       therapeutic_area,
       product_type,
       (case when upper(require_key_message) = 'true' then 1 when upper(require_key_message) = 'false' then 0 else 0 end) as require_key_message,
       external_id,
       manufacturer,
       (case when upper(company_product) = 'true' then 1 when upper(company_product) = 'false' then 0 else 0 end) as company_product,
       (case when upper(controlled_substance) = 'true' then 1 when upper(controlled_substance) = 'false' then 0 else 0 end) as controlled_substance,
       description,
       sample_quantity_picklist,
       display_order,
       (case when upper(no_metrics) = 'true' then 1 when upper(no_metrics) = 'false' then 0 else 0 end) as no_metrics,
       distributor,
       (case when upper(sample_quantity_bound) = 'true' then 1 when upper(sample_quantity_bound) = 'false' then 0 else 0 end) as sample_quantity_bound,
       sample_u_m,
       (case when upper(no_details) = 'true' then 1 when upper(no_details) = 'false' then 0 else 0 end) as no_details,
       (case when upper(restricted) = 'true' then 1 when upper(restricted) = 'false' then 0 else 0 end) as restricted,
       (case when upper(no_cycle_plans) = 'true' then 1 when upper(no_cycle_plans) = 'false' then 0 else 0 end) as no_cycle_plans,
       sku_id,
       business_unit,
       franchise,
       upper(country),
       biz_sub_unit,
       biz_unit,
       product_sector,
       cost,
       quantity_per_case,
       (case when upper(user_aligned) = 'true' then 1 when upper(user_aligned) = 'false' then 0 else 0 end) as user_aligned,
       restricted_states,
       sort_code,
       vexternal_id,
       product_identifier,
       (case when upper(imr) = 'true' then 1 when upper(imr) = 'false' then 0 else 0 end) as imr,
       detail_sub_type,
	   shc_sector,               
       shc_strategic_group,      
       shc_franchise,
       shc_brand,
       sysdate() as inserted_date,
       null as updated_date
from sdl_hcp_osea_product)
,
final as
(  select 
    	product_indication_key::varchar(32)  as product_indication_key,
	parent_product_key::varchar(32) as parent_product_key,
	product_source_id::varchar(18)  as product_source_id,
	owner_source_id::varchar(100) as owner_source_id,
	is_deleted::number(38,0) as is_deleted,
	product_name::varchar(80) as product_name,
	record_type_source_id::varchar(18) as record_type_source_id,
	created_date::timestamp_ntz(9) as created_date,
	created_by_id::varchar(18) as created_by_id,
	last_modified_date::timestamp_ntz(9) as last_modified_date,
	last_modified_by_id::varchar(18) as last_modified_by_id,
	consumer_site::varchar(255) as consumer_site,
	product_info::varchar(255) as product_info,
	therapeutic_class::varchar(255) as therapeutic_class,
	parent_product_source_id::varchar(18) as parent_product_source_id,
	therapeutic_area::varchar(255) as therapeutic_area,
	product_type::varchar(255) as product_type,
	require_key_message::number(38,0) as require_key_message,
	external_id::varchar(25) as external_id,
	manufacturer::varchar(255) as manufacturer,
	company_product::number(38,0) as company_product,
	controlled_substance::number(38,0) as controlled_substance,
	description::varchar(255) as description,
	sample_quantity_picklist::varchar(1000) as sample_quantity_picklist,
	display_order::number(5,0) as display_order,
	no_metrics::number(38,0) as no_metrics,
	distributor::varchar(255) as distributor,
	sample_quantity_bound::number(38,0) as sample_quantity_bound,
	sample_u_m::varchar(255) as sample_u_m,
	no_details::number(38,0) as no_details,
	restricted::number(38,0) as restricted,
	no_cycle_plans::number(38,0) as no_cycle_plans,
	sku_id::varchar(25) as sku_id,
	business_unit::varchar(4099) as business_unit,
	franchise::varchar(4099) as franchise,
	country_code::varchar(255)  as country_code,
	biz_sub_unit::varchar(255) as biz_sub_unit,
	biz_unit::varchar(255) as biz_unit,
	product_sector::varchar(1300) as product_sector,
	cost::number(14,2) as cost,
	quantity_per_case::number(10,1) as quantity_per_case,
	user_aligned::number(38,0) as user_aligned,
	restricted_states::varchar(100) as restricted_states,
	sort_code::varchar(20) as sort_code,
	vexternal_id::varchar(120) as vexternal_id,
	product_identifier::varchar(80) as product_identifier,
	imr::number(38,0) as imr,
	detail_sub_type::varchar(255) as detail_sub_type,
	shc_sector::varchar(255) as shc_sector,
	shc_strategic_group::varchar(255) as shc_strategic_group,
	shc_franchise::varchar(255) as shc_franchise,
	shc_brand::varchar(255) as shc_brand,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date
    from transformed
)

select * from final 





	