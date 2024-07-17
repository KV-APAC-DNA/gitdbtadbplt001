with v_rpt_sales_details as(
	select * from {{ ref('hcpedw_integration_v_rpt_sales_details') }}
),
itg_query_parameters as(
	select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
itg_mds_in_hcp_sales_hierarchy_mapping as(
	select * from {{ ref('hcpitg_integration__itg_mds_in_hcp_sales_hierarchy_mapping') }}
),
itg_mds_in_orsl_products_target as(
	select * from {{ ref('hcpitg_integration__itg_mds_in_orsl_products_target') }}
),
itg_mds_in_hcp_sales_rep_mapping as(
	select * from {{ ref('hcpitg_integration__itg_mds_in_hcp_sales_rep_mapping') }}
),
v_invoice_tde as(
	select * from {{ ref('hcpedw_integration__v_invoice_tde') }}
),    
itg_mds_in_hcp_sales_hierarchy_mapping as(
	select * from {{ ref('hcpitg_integration__itg_mds_in_hcp_sales_hierarchy_mapping') }}
),    
sales_data as
(
	select
        distinct upper(trim(map1.region_code)) as region,
        upper(trim(map1.zone_code)) as zone,
        upper(trim(map1.sales_area_code)) as hq,
        upper(trim(tde.source)) as source,
        tde.product_code,
        case
        when upper(trim(tgt.product_name)) is null then upper(trim(tde.product_name))
        else upper(trim(tgt.product_name))
        end as product_name,
        case
        when upper(trim(tgt.product_category_code)) is null then upper(trim(tde.product_category))
        else upper(trim(tgt.product_category_code))
        end as product_category,
        tde.fisc_yr as year,
        upper(trim(tde.month)) as month,
        tde.sum_achievement_nr as sales_value,
        temp.count_msr,
        tde.retailer_channel_1
    from
    (
        select
            'sales data' as source,
            customer_code,
            product_code,
            fisc_yr,
            month,
            product_name,
            product_category_name as product_category,
            case
            when sum(achievement_nr) = 0 then 0
            when sum(achievement_nr) <> 0 then sum(achievement_nr) / 1000
            end as sum_achievement_nr,
            upper(retailer_channel_1) as retailer_channel_1
        from
            v_rpt_sales_details
        where
            to_date(cast(mth_mm as varchar(20)), 'yyyymm') > to_date(
            add_months(
                convert_timezone('utc', current_timestamp()),
                -(
                select
                    cast(parameter_value as integer) as parameter_value
                from
                    itg_query_parameters
                where
                    upper(country_code) = 'in'
                    and upper(parameter_type) = 'orsl_reporting'
                    and upper(parameter_name) = 'orsl_reporting_date_range'
                )
            )
            )
            and product_code in (
            select
                parameter_value
            from
                itg_query_parameters
            where
                upper(country_code) = 'in'
                and upper(parameter_type) = 'orsl_reporting'
                and upper(parameter_name) = 'orsl_reporting_product_code'
            )
        group by
            customer_code,
            product_code,
            fisc_yr,
            month,
            product_name,
            product_category_name,
            retailer_channel_1
	) tde
	left join itg_mds_in_hcp_sales_hierarchy_mapping map1 on tde.customer_code = map1.rds_code
	and brand_name_code = 'orsl'
	left join itg_mds_in_orsl_products_target tgt on tde.product_code = tgt.product_code
	left join 
    (
        select
            hq_sales_area_code,
            count(1) as count_msr
        from
            itg_mds_in_hcp_sales_rep_mapping map1
        where
            designation = 'msr'
        group by
            map1.hq_sales_area_code
	) temp on upper (trim (map1.sales_area_code)) = upper (trim (temp.hq_sales_area_code))
)

,
invoicing_data as
	(select
      upper(trim(map1.region_code)) as region,
      upper(trim(map1.zone_code)) as zone,
      upper(trim(map1.sales_area_code)) as hq,
      'invoicing data' as source,
      inv.product_code,
      case
        when upper(trim(tgt.product_name)) is null then upper(trim(inv.product_name))
        else upper(trim(tgt.product_name))
      end as product_name,
      case
        when upper(trim(tgt.product_category_code)) is null then 'unknown'
        else upper(trim(tgt.product_category_code))
      end as product_category,
      inv.fisc_yr as year,
      upper(trim(inv.month_nm_shrt)) as month,
      case
        when sum(inv.invoice_val) = 0 then 0
        when sum(inv.invoice_val) <> 0 then sum(inv.invoice_val) / 1000
      end as sales_value,
      temp.count_msr,
      null as retailer_channel_1
    from
      v_invoice_tde inv
      left join itg_mds_in_orsl_products_target tgt on inv.product_code = cast (tgt.product_code as varchar (50))
      and upper (trim (inv.region_name)) = upper (trim (tgt.region_code))
      and upper (trim (inv.zone_name)) = upper (trim (tgt.zone_code))
      and upper (trim (inv.territory_name)) = upper (trim (tgt.hq_code))
      left join itg_mds_in_hcp_sales_hierarchy_mapping map1 on inv.customer_code = map1.rds_code
      and map1.brand_name_code = 'orsl'
      left join (
        select
          hq_sales_area_code,
          count(1) as count_msr
        from
          itg_mds_in_hcp_sales_rep_mapping map1
        where
          designation = 'msr'
        group by
          map1.hq_sales_area_code
      ) temp on upper (trim (map1.sales_area_code)) = upper (trim (temp.hq_sales_area_code))
    where
      inv.product_code in (
        select
          parameter_value
        from
          itg_query_parameters
        where
          upper(country_code) = 'in'
          and upper(parameter_type) = 'orsl_reporting'
          and upper(parameter_name) = 'orsl_reporting_product_code'
      )
    group by
      1,
      2,
      3,
      5,
      6,
      7,
      8,
      9,
      11
),
temp as
(
        select
          map1.hq_sales_area_code,count(*) as cnt
        from
          itg_mds_in_hcp_sales_rep_mapping map1,
        where
          map1.designation = 'msr'
        group by
          map1.hq_sales_area_code
)
,
target_data as
(
    select
      distinct upper(tgt.region_code) as region,
      upper(tgt.zone_code) as zone,
      upper(tgt.hq_code) as hq,
      'target data' as source,
      cast(tgt.product_code as varchar(50)),
      tgt.product_name,
      tgt.product_category_code as product_category,
      cast(tgt.year_code as integer) as year,
      upper(trim(tgt.month_code)) as month,
      tgt.target as sales_value,
      temp.cnt as count_msr,
      null as retailer_channel_1
    from
      itg_mds_in_orsl_products_target tgt
      left outer join itg_mds_in_hcp_sales_rep_mapping map on upper (trim (tgt.hq_code)) = upper (trim (map.hq_sales_area_code))
      left join temp
      on map.hq_sales_area_code = temp.hq_sales_area_code
)
,
transformed as
(
    select * from sales_data
    union  all
	select * from invoicing_data
    union all
    select * from target_data
),
final as
(
	select 
		region::varchar(500) as region,
		zone::varchar(500) as zone,
		hq::varchar(500) as hq,
		source::varchar(500) as source,
		product_code::varchar(50) as product_code,
		product_name::varchar(50) as product_name,
		product_category::varchar(200) as product_category,
		year::number(18,0) as year,
		month::varchar(10) as month,
		sales_value::number(38,6) as sales_value,
		count_msr::number(18,0) as count_msr,
		retailer_channel_1::varchar(200) as retailer_channel_1,
	from transformed
)
select * from final
