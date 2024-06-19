{{
    config
    (
        pre_hook = "{{build_itg_pop6_product_groups_lists()}}"
    )
}}

with sdl_pop6_hk_product_groups_lists AS
(
     select * from DEV_DNA_LOAD.SNAPNTASDL_RAW.SDL_POP6_HK_PRODUCT_GROUPS_LISTS
),

sdl_pop6_kr_product_groups_lists AS
(
     select * from DEV_DNA_LOAD.SNAPNTASDL_RAW.SDL_POP6_KR_PRODUCT_GROUPS_LISTS
),

sdl_pop6_tw_product_groups_lists as 
(
     select * from DEV_DNA_LOAD.SNAPNTASDL_RAW.SDL_POP6_TW_PRODUCT_GROUPS_LISTS
),


SDL
AS (
     SELECT 'HK' AS CNTRY_CD,
          SDL_HK.*
     FROM sdl_pop6_hk_product_groups_lists SDL_HK
     
     UNION ALL
     
     SELECT 'KR' AS CNTRY_CD,
          SDL_KR.*
     FROM sdl_pop6_kr_product_groups_lists SDL_KR
     
     UNION ALL
     
     SELECT 'TW' AS CNTRY_CD,
          SDL_TW.*
     FROM sdl_pop6_tw_product_groups_lists SDL_TW
     ),
wks
AS (
     SELECT prod_grp.cntry_cd,
          prod_grp.src_file_date,
          prod_grp.product_group_status,
          prod_grp.product_group,
          prod_grp.product_list_status,
          prod_grp.product_list,
          prod_grp.productdb_id,
          prod_grp.sku,
          prod_grp.prod_grp_date,
          prod_grp.custom_pop_attribute_1,
          prod_grp.custom_pop_attribute_2,
          prod_grp.custom_pop_attribute_3,
          prod_grp.hashkey,
          prod_grp.effective_from,
          CASE 
               WHEN prod_grp.effective_to IS NULL
                    THEN dateadd(DAY, - 1, current_timestamp()::timestamp_ntz(9))
               ELSE prod_grp.effective_to
               END AS effective_to,
          'N' AS active,
          prod_grp.file_name,
          prod_grp.run_id,
          prod_grp.crtd_dttm,
          prod_grp.updt_dttm
     FROM (
          SELECT itg.*
          FROM sdl,
               {{this}} itg
          WHERE sdl.hashkey != itg.hashkey
               AND sdl.productdb_id = itg.productdb_id
               AND sdl.product_list = itg.product_list
               AND nvl(sdl.custom_pop_attribute_1, 'NA') = nvl(itg.custom_pop_attribute_1, 'NA')
               AND nvl(sdl.custom_pop_attribute_2, 'NA') = nvl(itg.custom_pop_attribute_2, 'NA')
               AND nvl(sdl.custom_pop_attribute_3, 'NA') = nvl(itg.custom_pop_attribute_3, 'NA')
          ) prod_grp
     
     UNION ALL
     
     SELECT prod_grp.cntry_cd,
          SUBSTRING(prod_grp.file_name, 1, 8) AS src_file_date,
          prod_grp.product_group_status,
          prod_grp.product_group,
          prod_grp.product_list_status,
          prod_grp.product_list,
          prod_grp.productdb_id,
          prod_grp.sku,
          prod_grp.prod_grp_date,
          prod_grp.custom_pop_attribute_1,
          prod_grp.custom_pop_attribute_2,
          prod_grp.custom_pop_attribute_3,
          prod_grp.hashkey,
          current_timestamp()::timestamp_ntz(9) AS effective_from,
          NULL AS effective_to,
          'Y' AS active,
          prod_grp.file_name,
          run_id,
          prod_grp.crtd_dttm,
          current_timestamp()::timestamp_ntz(9) AS updt_dttm
     FROM (
          SELECT sdl.*
          FROM sdl,
               {{this}} itg
          WHERE sdl.hashkey != itg.hashkey
               AND sdl.productdb_id = itg.productdb_id
               AND sdl.product_list = itg.product_list
               AND nvl(sdl.custom_pop_attribute_1, 'NA') = nvl(itg.custom_pop_attribute_1, 'NA')
               AND nvl(sdl.custom_pop_attribute_2, 'NA') = nvl(itg.custom_pop_attribute_2, 'NA')
               AND nvl(sdl.custom_pop_attribute_3, 'NA') = nvl(itg.custom_pop_attribute_3, 'NA')
               AND itg.active = 'Y'
          ) prod_grp
     
     UNION ALL
     
     SELECT prod_grp.cntry_cd,
          SUBSTRING(prod_grp.file_name, 1, 8) AS src_file_date,
          prod_grp.product_group_status,
          prod_grp.product_group,
          prod_grp.product_list_status,
          prod_grp.product_list,
          prod_grp.productdb_id,
          prod_grp.sku,
          prod_grp.prod_grp_date,
          prod_grp.custom_pop_attribute_1,
          prod_grp.custom_pop_attribute_2,
          prod_grp.custom_pop_attribute_3,
          prod_grp.hashkey,
          prod_grp.effective_from,
          NULL AS effective_to,
          'Y' AS active,
          prod_grp.file_name,
          run_id,
          prod_grp.crtd_dttm,
          current_timestamp()::timestamp_ntz(9) AS updt_dttm
     FROM (
          SELECT sdl.*,
               itg.effective_from
          FROM sdl,
               {{this}} itg
          WHERE sdl.hashkey = itg.hashkey
               AND sdl.productdb_id = itg.productdb_id
               AND sdl.product_list = itg.product_list
               AND nvl(sdl.custom_pop_attribute_1, 'NA') = nvl(itg.custom_pop_attribute_1, 'NA')
               AND nvl(sdl.custom_pop_attribute_2, 'NA') = nvl(itg.custom_pop_attribute_2, 'NA')
               AND nvl(sdl.custom_pop_attribute_3, 'NA') = nvl(itg.custom_pop_attribute_3, 'NA')
               AND itg.active = 'Y'
          ) prod_grp
     
     UNION ALL
     
     SELECT prod_grp.cntry_cd,
          prod_grp.src_file_date,
          prod_grp.product_group_status,
          prod_grp.product_group,
          prod_grp.product_list_status,
          prod_grp.product_list,
          prod_grp.productdb_id,
          prod_grp.sku,
          prod_grp.prod_grp_date,
          prod_grp.custom_pop_attribute_1,
          prod_grp.custom_pop_attribute_2,
          prod_grp.custom_pop_attribute_3,
          prod_grp.hashkey,
          prod_grp.effective_from,
          prod_grp.effective_to,
          prod_grp.active,
          prod_grp.file_name,
          prod_grp.run_id,
          prod_grp.crtd_dttm,
          prod_grp.updt_dttm
     FROM (
          SELECT itg.*
          FROM sdl,
               {{this}} itg
          WHERE sdl.hashkey = itg.hashkey
               AND sdl.productdb_id = itg.productdb_id
               AND sdl.product_list = itg.product_list
               AND nvl(sdl.custom_pop_attribute_1, 'NA') = nvl(itg.custom_pop_attribute_1, 'NA')
               AND nvl(sdl.custom_pop_attribute_2, 'NA') = nvl(itg.custom_pop_attribute_2, 'NA')
               AND nvl(sdl.custom_pop_attribute_3, 'NA') = nvl(itg.custom_pop_attribute_3, 'NA')
               AND itg.active = 'N'
          ) prod_grp
     
     UNION ALL
     
     SELECT prod_grp.cntry_cd,
          SUBSTRING(prod_grp.file_name, 1, 8) AS src_file_date,
          prod_grp.product_group_status,
          prod_grp.product_group,
          prod_grp.product_list_status,
          prod_grp.product_list,
          prod_grp.productdb_id,
          prod_grp.sku,
          prod_grp.prod_grp_date,
          prod_grp.custom_pop_attribute_1,
          prod_grp.custom_pop_attribute_2,
          prod_grp.custom_pop_attribute_3,
          prod_grp.hashkey,
          current_timestamp()::timestamp_ntz(9) AS effective_from,
          NULL AS effective_to,
          'Y' AS active,
          prod_grp.file_name,
          run_id,
          prod_grp.crtd_dttm,
          current_timestamp()::timestamp_ntz(9) AS updt_dttm
     FROM (
          SELECT *
          FROM sdl
          WHERE productdb_id || product_list || nvl(custom_pop_attribute_1, 'NA') || nvl(custom_pop_attribute_2, 'NA') || nvl(custom_pop_attribute_3, 'NA') NOT IN (
                    SELECT productdb_id || product_list || nvl(custom_pop_attribute_1, 'NA') || nvl(custom_pop_attribute_2, 'NA') || nvl(custom_pop_attribute_3, 'NA')
                    FROM {{this}}
                    )
          ) prod_grp
     ),
transformed
AS (
     SELECT *
     FROM wks
     
     UNION ALL
     
     SELECT *
     FROM {{this}}
     WHERE productdb_id || product_list || nvl(custom_pop_attribute_1, 'NA') || nvl(custom_pop_attribute_2, 'NA') || nvl(custom_pop_attribute_3, 'NA') NOT IN (
               SELECT productdb_id || product_list || nvl(custom_pop_attribute_1, 'NA') || nvl(custom_pop_attribute_2, 'NA') || nvl(custom_pop_attribute_3, 'NA')
               FROM wks
               )
     ),


final as
(
     select
          cntry_cd::varchar(10) as cntry_cd,
          src_file_date::varchar(10) as src_file_date,
          product_group_status::number(18,0) as product_group_status,
          product_group::varchar(25) as product_group,
          product_list_status::number(18,0) as product_list_status,
          product_list::varchar(30) as product_list,
          productdb_id::varchar(255) as productdb_id,
          sku::varchar(150) as sku,
          prod_grp_date::date as prod_grp_date,
          custom_pop_attribute_1::varchar(200) as custom_pop_attribute_1,
          custom_pop_attribute_2::varchar(200) as custom_pop_attribute_2,
          custom_pop_attribute_3::varchar(200) as custom_pop_attribute_3,
          hashkey::varchar(200) as hashkey,
          effective_from::timestamp_ntz(9) as effective_from,
          effective_to::timestamp_ntz(9) as effective_to,
          active::varchar(2) as active,
          file_name::varchar(100) as file_name,
          run_id::number(14,0) as run_id,
          current_timestamp()::timestamp_ntz(9) as crtd_dttm,
          current_timestamp()::timestamp_ntz(9) as updt_dttm
     from transformed
)
select * from final