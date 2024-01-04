{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "table",
        transient= false
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_edw_gch_customerhierarchy') }}
),

--Logical CTE

--Final CTE
final as (
    select
  tamr_id,
  origin_source_name,
  origin_entity_id,
  manualclassificationid,
  manualclassificationpath,
  suggestedclassificationid,
  suggestedclassificationpath,
  suggestedclassificationscore,
  finalclassificationpath,
  unique_id,
  region,
  customer,
  country,
  name,
  city,
  postal_code,
  state,
  district,
  search_term,
  regional_customer,
  regional_customer_code,
  regional_banner,
  regional_banner_code,
  regional_channel,
  regional_segmentation,
  regional_class_of_trade,
  emea_top_20_key_account,
  apac_global_customer,
  apac_go_to_model,
  apac_sub_channel,
  apac_banner_in_format,
  latam_regional_customer_group,
  gcgh_region,
  gcgh_cluster,
  gcgh_subcluster,
  gcgh_market,
  gcch_customer,
  gcch_retail_banner,
  dateofextract,
  cdl_datetime,
  cdl_source_file,
  load_key,
  current_timestamp()::timestamp_ntz(9) as crt_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm,
  null as null,
  null as null
  from source)


--Final select
select * from final 