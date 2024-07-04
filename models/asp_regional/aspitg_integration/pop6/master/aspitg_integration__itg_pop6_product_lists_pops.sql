{{
    config(
        pre_hook= '{{build_itg_pop6_product_lists_pops_temp()}}'
    )
}}

with sdl_pop6_hk_product_lists_pops as (
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_product_lists_pops') }}
),
sdl_pop6_kr_product_lists_pops as (
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_product_lists_pops') }}
),
sdl_pop6_tw_product_lists_pops as (
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_product_lists_pops') }}
),
sdl_pop6_jp_product_lists_pops as (
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_product_lists_pops') }}
),
sdl_pop6_sg_product_lists_pops as (
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_product_lists_pops') }}
),
sdl_pop6_th_product_lists_pops as (
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_product_lists_pops') }}
),
itg_pop6_product_lists_pops as (
    select * from {{ source('ntaitg_integration', 'itg_pop6_product_lists_pops_temp') }}
),
SDL AS
		(SELECT 'HK' AS CNTRY_CD,
				   SDL_HK.*
			FROM sdl_pop6_hk_product_lists_pops SDL_HK
			UNION ALL
			SELECT 'KR' AS CNTRY_CD,
				   SDL_KR.*
			FROM sdl_pop6_kr_product_lists_pops SDL_KR
			UNION ALL
			SELECT 'TW' AS CNTRY_CD,
				   SDL_TW.*
			FROM sdl_pop6_tw_product_lists_pops SDL_TW
			UNION ALL
			SELECT 'JP' AS CNTRY_CD,
				   SDL_JP.*
			FROM sdl_pop6_jp_product_lists_pops SDL_JP
			UNION ALL
			SELECT 'SG' AS CNTRY_CD,
				   SDL_SG.*
			FROM sdl_pop6_sg_product_lists_pops SDL_SG
						UNION ALL
			SELECT 'TH' AS CNTRY_CD,
				   SDL_TH.*
			FROM sdl_pop6_th_product_lists_pops SDL_TH),
wks as (
SELECT poplist.cntry_cd,
       poplist.src_file_date,
       poplist.product_list,
       poplist.popdb_id,
       poplist.pop_code,
       poplist.pop_name,
       poplist.prod_grp_date,
       poplist.hashkey,
       poplist.effective_from,
       CASE WHEN poplist.effective_to IS NULL
			THEN dateadd(DAY,-1,convert_timezone ('Asia/Singapore',current_timestamp())::TIMESTAMP_NTZ)
			ELSE poplist.effective_to
	   END AS effective_to,
       'N' AS active,
       poplist.file_name,
       poplist.run_id,
       poplist.crtd_dttm,
       poplist.updt_dttm
FROM (SELECT itg.*
      FROM sdl,
           itg_pop6_product_lists_pops itg
      WHERE sdl.hashkey != itg.hashkey
      AND   sdl.product_list = itg.product_list
	  AND	sdl.popdb_id = itg.popdb_id) poplist
UNION ALL
SELECT poplist.cntry_cd,
       SUBSTRING(poplist.file_name,1,8) AS src_file_date,
       poplist.product_list,
       poplist.popdb_id,
       poplist.pop_code,
       poplist.pop_name,
       poplist.prod_grp_date,
       poplist.hashkey,
       convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9) AS effective_from,
       NULL AS effective_to,
       'Y' AS active,
       poplist.file_name,
       null as run_id,
       poplist.crtd_dttm,
       convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9) AS updt_dttm
FROM (SELECT sdl.*
      FROM sdl,
           itg_pop6_product_lists_pops itg
      WHERE sdl.hashkey != itg.hashkey
      AND   sdl.product_list = itg.product_list
	  AND	sdl.popdb_id = itg.popdb_id
	  AND	itg.active = 'Y') poplist
UNION ALL
SELECT poplist.cntry_cd,
       SUBSTRING(poplist.file_name,1,8) AS src_file_date,
       poplist.product_list,
       poplist.popdb_id,
       poplist.pop_code,
       poplist.pop_name,
       poplist.prod_grp_date,
       poplist.hashkey,
       poplist.effective_from,
       NULL AS effective_to,
       'Y' AS active,
       poplist.file_name,
       null as run_id,
       poplist.crtd_dttm,
       convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9) AS updt_dttm
FROM (SELECT sdl.*,itg.effective_from
      FROM sdl,
           itg_pop6_product_lists_pops itg
      WHERE sdl.hashkey = itg.hashkey
      AND   sdl.product_list = itg.product_list
	  AND	sdl.popdb_id = itg.popdb_id
	  AND	itg.active = 'Y') poplist	
UNION ALL
SELECT poplist.cntry_cd,
       poplist.src_file_date,
       poplist.product_list,
       poplist.popdb_id,
       poplist.pop_code,
       poplist.pop_name,
       poplist.prod_grp_date,
       poplist.hashkey,
       poplist.effective_from,
       poplist.effective_to,
       poplist.active,
       poplist.file_name,
       poplist.run_id,
       poplist.crtd_dttm,
       poplist.updt_dttm
FROM (SELECT itg.*
      FROM sdl,
           itg_pop6_product_lists_pops itg
      WHERE sdl.hashkey = itg.hashkey
      AND   sdl.product_list = itg.product_list
	  AND	sdl.popdb_id = itg.popdb_id
	  AND	itg.active = 'N') poplist	  
	  
UNION ALL
SELECT poplist.cntry_cd,
       SUBSTRING(poplist.file_name,1,8) AS src_file_date,
       poplist.product_list,
       poplist.popdb_id,
       poplist.pop_code,
       poplist.pop_name,
       poplist.prod_grp_date,
       poplist.hashkey,
       convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9) AS effective_from,
       NULL AS effective_to,
       'Y' AS active,
       poplist.file_name,
       null as run_id,
       poplist.crtd_dttm,
       convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz(9) AS updt_dttm
FROM (SELECT *
      FROM sdl
      WHERE product_list||popdb_id NOT IN (SELECT product_list||popdb_id FROM itg_pop6_product_lists_pops)) poplist),
transformed as (
SELECT*
FROM wks
UNION ALL
SELECT *
FROM itg_pop6_product_lists_pops
WHERE product_list||popdb_id NOT IN (SELECT product_list||popdb_id FROM wks)),
final as (
select
cntry_cd::varchar(10) as cntry_cd,
src_file_date::varchar(10) as src_file_date,
product_list::varchar(100) as product_list,
popdb_id::varchar(255) as popdb_id,
pop_code::varchar(50) as pop_code,
pop_name::varchar(100) as pop_name,
prod_grp_date::date as prod_grp_date,
hashkey::varchar(200) as hashkey,
effective_from::timestamp_ntz(9) as effective_from,
effective_to::timestamp_ntz(9) as effective_to,
active::varchar(2) as active,
file_name::varchar(100) as file_name,
run_id::number(14,0) as run_id,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final
