{{
    config(
        pre_hook= '{{build_itg_pop6_pops_temp()}}'
    )
}}

with sdl_pop6_JP_pops as (
    select * from DEV_DNA_LOAD.SNAPJPNSDL_RAW.SDL_POP6_JP_POPS
),
sdl_pop6_hk_pops as (
    select * from DEV_DNA_LOAD.SNAPNTASDL_RAW.SDL_POP6_HK_POPS
),
sdl_pop6_kr_pops as (
    select * from DEV_DNA_LOAD.SNAPNTASDL_RAW.sdl_pop6_kr_pops
),
sdl_pop6_tw_pops as (
    select * from DEV_DNA_LOAD.SNAPNTASDL_RAW.sdl_pop6_tw_pops
),
sdl_pop6_sg_pops as (
    select * from DEV_DNA_LOAD.SNAPOSESDL_RAW.SDL_POP6_SG_POPS
),
sdl_pop6_th_pops as (
    select * from DEV_DNA_LOAD.SNAPOSESDL_RAW.sdl_pop6_th_pops
),
itg_pop6_pops_temp as (
    select * from {{ source('ntaitg_integration', 'ntaitg_integration__itg_pop6_pops') }}
),
wks as
(WITH SDL
AS
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
) 

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
       null as run_id,
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
       null as run_id,
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
      null as run_id,
       pops.crtd_dttm,
       convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9) AS updt_dttm,
       business_units_id,
business_unit_name,
territory_or_region
FROM (SELECT *
      FROM sdl
      WHERE popdb_id NOT IN (SELECT popdb_id FROM itg_pop6_pops_temp)) pops) 
     
SELECT *
FROM wks

UNION ALL

SELECT *
FROM itg_pop6_pops_temp
WHERE popdb_id NOT IN (SELECT popdb_id FROM wks)