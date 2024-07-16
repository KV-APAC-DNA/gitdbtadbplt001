{{
    config(
        pre_hook= '{{build_itg_pop6_pops_temp()}}'
    )
}}

with sdl_pop6_jp_pops as (
    select * from {{ref('aspwks_integration__wks_pop6_jp_pops')}}
),
sdl_pop6_hk_pops as (
    select * from {{ref('aspwks_integration__wks_pop6_hk_pops')}}
),
sdl_pop6_kr_pops as (
    select * from {{ref('aspwks_integration__wks_pop6_kr_pops')}}
),
sdl_pop6_tw_pops as (
    select * from {{ref('aspwks_integration__wks_pop6_tw_pops')}}
),
sdl_pop6_sg_pops as (
    select * from {{ref('aspwks_integration__wks_pop6_sg_pops')}}
),
sdl_pop6_th_pops as (
    select * from {{ref('aspwks_integration__wks_pop6_th_pops')}}
),
itg_pop6_pops_temp as (
    select * from {{source('aspitg_integration', 'itg_pop6_pops_temp')}}
),
SDL as
(SELECT 'HK' AS CNTRY_CD,
       status,
       popdb_id,
       pop_code,
       pop_name,
       address,
       longitude,
       latitude,
       country,
       channel,
       retail_environment_ps,
       customer,
       sales_group_code,
       sales_group_name,
       customer_grade,
       NULL AS external_pop_code,
       file_name,
       run_id,
       crtd_dttm,
       hashkey,
 business_units_id,
business_unit_name,
 territory_or_region
FROM sdl_pop6_hk_pops SDL_HK
UNION ALL
SELECT 'KR' AS CNTRY_CD,
       status,
       popdb_id,
       pop_code,
       pop_name,
       address,
       longitude,
       latitude,
       country,
       channel,
       retail_environment_ps,
       customer,
       sales_group_code,
       sales_group_name,
       customer_grade,
       NULL AS external_pop_code,
       file_name,
       run_id,
       crtd_dttm,
       hashkey,
business_units_id,
business_unit_name,
 territory_or_region
FROM sdl_pop6_kr_pops SDL_KR
UNION ALL
SELECT 'TW' AS CNTRY_CD,
       status,
       popdb_id,
       pop_code,
       pop_name,
       address,
       longitude,
       latitude,
       country,
       channel,
       retail_environment_ps,
       customer,
       sales_group_code,
       sales_group_name,
       customer_grade,
       external_pop_code,
       file_name,
       run_id,
       crtd_dttm,
       hashkey,
 business_units_id,
  business_unit_name,
 territory_or_region
FROM sdl_pop6_tw_pops SDL_TW
UNION ALL
SELECT 'JP' AS CNTRY_CD,
       status,
       popdb_id,
       pop_code,
       pop_name,
       address,
       longitude,
       latitude,
       country,
       channel,
       retail_environment_ps,
       customer,
       sales_group_code,
       sales_group_name,
       customer_grade,
       NULL AS external_pop_code,
       file_name,
       run_id,
       crtd_dttm,
       hashkey,
business_units_id,
business_unit_name,
territory_or_region
FROM sdl_pop6_JP_pops SDL_JP
UNION ALL
SELECT 'SG' AS CNTRY_CD,
       status,
       popdb_id,
       pop_code,
       pop_name,
       address,
       longitude,
       latitude,
       country,
       channel,
       retail_environment_ps,
       customer,
       sales_group_code,
       sales_group_name,
       customer_grade,
       NULL AS external_pop_code,
       file_name,
       run_id,
       crtd_dttm,
       hashkey,
business_units_id,
business_unit_name,
territory_or_region
FROM sdl_pop6_sg_pops SDL_SG
UNION
SELECT 'TH' AS CNTRY_CD,
       status,
       popdb_id,
       pop_code,
       pop_name,
       address,
       longitude,
       latitude,
       country,
       channel,
       retail_environment_ps,
       customer,
       sales_group_code,
       sales_group_name,
       customer_grade,
       NULL AS external_pop_code,
       file_name,
       run_id,
       crtd_dttm,
       hashkey,
business_units_id,
business_unit_name,
territory_or_region
FROM sdl_pop6_th_pops
) ,
wks as (
SELECT pops.cntry_cd,
       pops.src_file_date,
       pops.status,
       pops.popdb_id,
       pops.pop_code,
       pops.pop_name,
       pops.address,
       pops.longitude,
       pops.latitude,
       pops.country,
       pops.channel,
       pops.retail_environment_ps,
       pops.customer,
       pops.sales_group_code,
       pops.sales_group_name,
       pops.customer_grade,
       pops.external_pop_code,
       pops.hashkey,
       pops.effective_from,
       CASE WHEN pops.effective_to IS NULL
			THEN dateadd(DAY,-1,convert_timezone ('Asia/Singapore',current_timestamp()))
			ELSE pops.effective_to
	   END AS effective_to,
       'N' AS active,
       pops.file_name,
       pops.run_id,
       pops.crtd_dttm,
       pops.updt_dttm,
       business_units_id,
business_unit_name,
territory_or_region
FROM (SELECT itg.*
      FROM sdl,
           itg_pop6_pops_temp itg
      WHERE sdl.hashkey != itg.hashkey
      AND   sdl.popdb_id = itg.popdb_id) pops
UNION ALL
SELECT pops.cntry_cd,
       SUBSTRING(pops.file_name,1,8) AS src_file_date,
       pops.status,
       pops.popdb_id,
       pops.pop_code,
       pops.pop_name,
       pops.address,
       pops.longitude,
       pops.latitude,
       pops.country,
       pops.channel,
       pops.retail_environment_ps,
       pops.customer,
       pops.sales_group_code,
       pops.sales_group_name,
       pops.customer_grade,
       pops.external_pop_code,
       pops.hashkey,
       convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9) AS effective_from,
       NULL AS effective_to,
       'Y' AS active,
       pops.file_name,
       run_id as run_id,
       pops.crtd_dttm,
       convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9) AS updt_dttm,
       business_units_id,
business_unit_name,
territory_or_region
FROM (SELECT sdl.*
      FROM sdl,
           itg_pop6_pops_temp itg
      WHERE sdl.hashkey != itg.hashkey
      AND   sdl.popdb_id = itg.popdb_id
	  AND	itg.active = 'Y') pops
UNION ALL
SELECT pops.cntry_cd,
       SUBSTRING(pops.file_name,1,8) AS src_file_date,
       pops.status,
       pops.popdb_id,
       pops.pop_code,
       pops.pop_name,
       pops.address,
       pops.longitude,
       pops.latitude,
       pops.country,
       pops.channel,
       pops.retail_environment_ps,
       pops.customer,
       pops.sales_group_code,
       pops.sales_group_name,
       pops.customer_grade,
       pops.external_pop_code,
       pops.hashkey,
       pops.effective_from,
       NULL AS effective_to,
       'Y' AS active,
       pops.file_name,
       run_id as run_id,
       pops.crtd_dttm,
       convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9) AS updt_dttm,
       business_units_id,
business_unit_name,
territory_or_region
FROM (SELECT sdl.*,itg.effective_from
      FROM sdl,
           itg_pop6_pops_temp itg
      WHERE sdl.hashkey = itg.hashkey
      AND   sdl.popdb_id = itg.popdb_id
	  AND	itg.active = 'Y') pops
UNION ALL
SELECT pops.cntry_cd,
       pops.src_file_date,
       pops.status,
       pops.popdb_id,
       pops.pop_code,
       pops.pop_name,
       pops.address,
       pops.longitude,
       pops.latitude,
       pops.country,
       pops.channel,
       pops.retail_environment_ps,
       pops.customer,
       pops.sales_group_code,
       pops.sales_group_name,
       pops.customer_grade,
       pops.external_pop_code,
       pops.hashkey,
       pops.effective_from,
       pops.effective_to,
       pops.active,
       pops.file_name,
       pops.run_id,
       pops.crtd_dttm,
       pops.updt_dttm,
       business_units_id,
business_unit_name,
territory_or_region
FROM (SELECT itg.*
      FROM sdl,
           itg_pop6_pops_temp itg
      WHERE sdl.hashkey = itg.hashkey
      AND   sdl.popdb_id = itg.popdb_id
	  AND	itg.active = 'N') pops
UNION ALL
SELECT cntry_cd,
       SUBSTRING(pops.file_name,1,8) AS src_file_date,
       pops.status,
       pops.popdb_id,
       pops.pop_code,
       pops.pop_name,
       pops.address,
       pops.longitude,
       pops.latitude,
       pops.country,
       pops.channel,
       pops.retail_environment_ps,
       pops.customer,
       pops.sales_group_code,
       pops.sales_group_name,
       pops.customer_grade,
       pops.external_pop_code,
       pops.hashkey,
       convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9) AS effective_from,
       NULL AS effective_to,
       'Y' AS active,
       pops.file_name,
      run_id as run_id,
       pops.crtd_dttm,
       convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9) AS updt_dttm,
       business_units_id,
business_unit_name,
territory_or_region
FROM (SELECT *
      FROM sdl
      WHERE popdb_id NOT IN (SELECT popdb_id FROM itg_pop6_pops_temp)) pops) ,
transformed as (
SELECT *
FROM wks

UNION ALL

SELECT *
FROM itg_pop6_pops_temp
WHERE popdb_id NOT IN (SELECT popdb_id FROM wks)),
final as (
SELECT
cntry_cd::varchar(10) as cntry_cd,
src_file_date::varchar(32) as src_file_date,
status::number(18,0) as status,
popdb_id::varchar(255) as popdb_id,
pop_code::varchar(50) as pop_code,
pop_name::varchar(100) as pop_name,
address::varchar(255) as address,
longitude::number(18,5) as longitude,
latitude::number(18,5) as latitude,
country::varchar(200) as country,
channel::varchar(200) as channel,
retail_environment_ps::varchar(200) as retail_environment_ps,
customer::varchar(200) as customer,
sales_group_code::varchar(200) as sales_group_code,
sales_group_name::varchar(200) as sales_group_name,
customer_grade::varchar(200) as customer_grade,
external_pop_code::varchar(200) as external_pop_code,
hashkey::varchar(200) as hashkey,
effective_from::timestamp_ntz(9) as effective_from,
effective_to::timestamp_ntz(9) as effective_to,
active::varchar(2) as active,
file_name::varchar(100) as file_name,
run_id::number(19,0) as run_id,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm,
business_units_id::varchar(255) as business_units_id,
business_unit_name::varchar(255) as business_unit_name,
territory_or_region::varchar(200) as territory_or_region
FROM transformed)
select * from final