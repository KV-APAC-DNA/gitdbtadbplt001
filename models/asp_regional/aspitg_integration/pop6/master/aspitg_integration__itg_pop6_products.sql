{{
    config(
        pre_hook= '{{build_itg_pop6_products_temp()}}'
    )
}}

with sdl_pop6_hk_products as (
    select * from {{ ref('aspwks_integration__wks_pop6_hk_products') }}
),
sdl_pop6_kr_products as (
    select * from {{ ref('aspwks_integration__wks_pop6_kr_products') }}
),
sdl_pop6_tw_products as (
    select * from {{ ref('aspwks_integration__wks_pop6_tw_products') }}
),
sdl_pop6_jp_products as (
    select * from {{ ref('aspwks_integration__wks_pop6_jp_products') }}
),
sdl_pop6_sg_products as (
    select * from {{ ref('aspwks_integration__wks_pop6_sg_products') }}
),
sdl_pop6_th_products as (
    select *from {{ ref('aspwks_integration__wks_pop6_th_products') }}
),
itg_pop6_products as (
    select * from {{ source('aspitg_integration', 'itg_pop6_products_temp') }}
),
SDL AS (
    SELECT 'HK' AS CNTRY_CD,
        SDL_HK.*
    FROM SDL_POP6_HK_PRODUCTS SDL_HK
    UNION ALL
    SELECT 'KR' AS CNTRY_CD,
        SDL_KR.status,
        SDL_KR.productdb_id,
        SDL_KR.barcode,
        SDL_KR.sku,
        SDL_KR.unit_price,
        SDL_KR.display_order,
        SDL_KR.launch_date,
        SDL_KR.largest_uom_quantity,
        SDL_KR.middle_uom_quantity,
        SDL_KR.smallest_uom_quantity,
        SDL_KR.company,
        SDL_KR.sku_english,
        SDL_KR.sku_code,
        SDL_KR.ps_category,
        SDL_KR.ps_segment,
        SDL_KR.ps_category_segment,
        SDL_KR.country_l1,
        SDL_KR.regional_franchise_l2,
        SDL_KR.franchise_l3,
        SDL_KR.brand_l4,
        SDL_KR.sub_category_l5,
        SDL_KR.platform_l6,
        SDL_KR.variance_l7,
        SDL_KR.pack_size_l8,
        SDL_KR.file_name,
        SDL_KR.run_id,
        SDL_KR.crtd_dttm,
        SDL_KR.hashkey
    FROM SDL_POP6_KR_PRODUCTS SDL_KR
    UNION ALL
    SELECT 'TW' AS CNTRY_CD,
        SDL_TW.*
    FROM SDL_POP6_TW_PRODUCTS SDL_TW
    UNION ALL
    SELECT 'JP' AS CNTRY_CD,
        SDL_jp.*
    FROM SDL_POP6_jp_PRODUCTS SDL_jp
    UNION ALL
    SELECT 'SG' AS CNTRY_CD,
        SDL_sg.*
    FROM SDL_POP6_sg_PRODUCTS SDL_sg
    UNION ALL
    SELECT 'TH' AS CNTRY_CD,
        SDL_TH.*
    FROM SDL_POP6_TH_PRODUCTS SDL_TH
),
wks as (
    SELECT prod.cntry_cd,
        prod.src_file_date,
        prod.status,
        prod.productdb_id,
        prod.barcode,
        prod.sku,
        prod.unit_price,
        prod.display_order,
        prod.launch_date,
        prod.largest_uom_quantity,
        prod.middle_uom_quantity,
        prod.smallest_uom_quantity,
        prod.company,
        prod.sku_english,
        prod.sku_code,
        prod.ps_category,
        prod.ps_segment,
        prod.ps_category_segment,
        prod.country_l1,
        prod.regional_franchise_l2,
        prod.franchise_l3,
        prod.brand_l4,
        prod.sub_category_l5,
        prod.platform_l6,
        prod.variance_l7,
        prod.pack_size_l8,
        prod.hashkey,
        prod.effective_from,
        CASE
            WHEN prod.effective_to IS NULL THEN dateadd(
                DAY,
                -1,
                convert_timezone('Asia/Singapore', current_timestamp())::timestamp_ntz(9)
            )
            ELSE prod.effective_to
        END AS effective_to,
        'N' AS active,
        prod.file_name,
        prod.run_id,
        prod.crtd_dttm,
        prod.updt_dttm
    FROM (
            SELECT itg.*
            FROM sdl,
                itg_pop6_products itg
            WHERE sdl.hashkey != itg.hashkey
                AND sdl.productdb_id = itg.productdb_id
        ) prod
    UNION ALL
    SELECT prod.cntry_cd,
        SUBSTRING(prod.file_name, 1, 8) AS src_file_date,
        prod.status,
        prod.productdb_id,
        prod.barcode,
        prod.sku,
        prod.unit_price,
        prod.display_order,
        prod.launch_date,
        prod.largest_uom_quantity,
        prod.middle_uom_quantity,
        prod.smallest_uom_quantity,
        prod.company,
        prod.sku_english,
        prod.sku_code,
        prod.ps_category,
        prod.ps_segment,
        prod.ps_category_segment,
        prod.country_l1,
        prod.regional_franchise_l2,
        prod.franchise_l3,
        prod.brand_l4,
        prod.sub_category_l5,
        prod.platform_l6,
        prod.variance_l7,
        prod.pack_size_l8,
        prod.hashkey,
        convert_timezone('Asia/Singapore', current_timestamp())::timestamp_ntz(9) AS effective_from,
        NULL AS effective_to,
        'Y' AS active,
        prod.file_name,
        run_id as run_id,
        prod.crtd_dttm,
        convert_timezone('Asia/Singapore', current_timestamp())::timestamp_ntz(9) AS updt_dttm
    FROM (
            SELECT sdl.*
            FROM sdl,
                itg_pop6_products itg
            WHERE sdl.hashkey != itg.hashkey
                AND sdl.productdb_id = itg.productdb_id
                AND itg.active = 'Y'
        ) prod
    UNION ALL
    SELECT prod.cntry_cd,
        SUBSTRING(prod.file_name, 1, 8) AS src_file_date,
        prod.status,
        prod.productdb_id,
        prod.barcode,
        prod.sku,
        prod.unit_price,
        prod.display_order,
        prod.launch_date,
        prod.largest_uom_quantity,
        prod.middle_uom_quantity,
        prod.smallest_uom_quantity,
        prod.company,
        prod.sku_english,
        prod.sku_code,
        prod.ps_category,
        prod.ps_segment,
        prod.ps_category_segment,
        prod.country_l1,
        prod.regional_franchise_l2,
        prod.franchise_l3,
        prod.brand_l4,
        prod.sub_category_l5,
        prod.platform_l6,
        prod.variance_l7,
        prod.pack_size_l8,
        prod.hashkey,
        prod.effective_from,
        NULL AS effective_to,
        'Y' AS active,
        prod.file_name,
        run_id as run_id,
        prod.crtd_dttm,
        convert_timezone('Asia/Singapore', current_timestamp())::timestamp_ntz(9) AS updt_dttm
    FROM (
            SELECT sdl.*,
                itg.effective_from
            FROM sdl,
                itg_pop6_products itg
            WHERE sdl.hashkey = itg.hashkey
                AND sdl.productdb_id = itg.productdb_id
                AND itg.active = 'Y'
        ) prod
    UNION ALL
    SELECT prod.cntry_cd,
        prod.src_file_date,
        prod.status,
        prod.productdb_id,
        prod.barcode,
        prod.sku,
        prod.unit_price,
        prod.display_order,
        prod.launch_date,
        prod.largest_uom_quantity,
        prod.middle_uom_quantity,
        prod.smallest_uom_quantity,
        prod.company,
        prod.sku_english,
        prod.sku_code,
        prod.ps_category,
        prod.ps_segment,
        prod.ps_category_segment,
        prod.country_l1,
        prod.regional_franchise_l2,
        prod.franchise_l3,
        prod.brand_l4,
        prod.sub_category_l5,
        prod.platform_l6,
        prod.variance_l7,
        prod.pack_size_l8,
        prod.hashkey,
        prod.effective_from,
        prod.effective_to,
        prod.active,
        prod.file_name,
        prod.run_id,
        prod.crtd_dttm,
        prod.updt_dttm
    FROM (
            SELECT itg.*
            FROM sdl,
                itg_pop6_products itg
            WHERE sdl.hashkey = itg.hashkey
                AND sdl.productdb_id = itg.productdb_id
                AND itg.active = 'N'
        ) prod
    UNION ALL
    SELECT prod.cntry_cd,
        SUBSTRING(prod.file_name, 1, 8) AS src_file_date,
        prod.status,
        prod.productdb_id,
        prod.barcode,
        prod.sku,
        prod.unit_price,
        prod.display_order,
        prod.launch_date,
        prod.largest_uom_quantity,
        prod.middle_uom_quantity,
        prod.smallest_uom_quantity,
        prod.company,
        prod.sku_english,
        prod.sku_code,
        prod.ps_category,
        prod.ps_segment,
        prod.ps_category_segment,
        prod.country_l1,
        prod.regional_franchise_l2,
        prod.franchise_l3,
        prod.brand_l4,
        prod.sub_category_l5,
        prod.platform_l6,
        prod.variance_l7,
        prod.pack_size_l8,
        prod.hashkey,
        convert_timezone('Asia/Singapore', current_timestamp())::timestamp_ntz(9) AS effective_from,
        NULL AS effective_to,
        'Y' AS active,
        prod.file_name,
        run_id as run_id,
        prod.crtd_dttm,
        convert_timezone('Asia/Singapore', current_timestamp())::timestamp_ntz(9) AS updt_dttm
    FROM (
            SELECT *
            FROM sdl
            WHERE productdb_id NOT IN (
                    SELECT productdb_id
                    FROM itg_pop6_products
                )
        ) prod
),
transformed as (
    SELECT * FROM wks
    UNION ALL
    SELECT * FROM itg_pop6_products
    WHERE productdb_id NOT IN (
            SELECT productdb_id
            FROM wks
        )
),
final as (
    select cntry_cd::varchar(10) as cntry_cd,
        src_file_date::varchar(10) as src_file_date,
        status::number(18, 0) as status,
        productdb_id::varchar(255) as productdb_id,
        barcode::varchar(150) as barcode,
        sku::varchar(150) as sku,
        unit_price::number(18, 2) as unit_price,
        display_order::number(18, 0) as display_order,
        launch_date::varchar(20) as launch_date,
        largest_uom_quantity::number(18, 0) as largest_uom_quantity,
        middle_uom_quantity::number(18, 0) as middle_uom_quantity,
        smallest_uom_quantity::number(18, 0) as smallest_uom_quantity,
        company::varchar(200) as company,
        sku_english::varchar(200) as sku_english,
        sku_code::varchar(200) as sku_code,
        ps_category::varchar(200) as ps_category,
        ps_segment::varchar(200) as ps_segment,
        ps_category_segment::varchar(200) as ps_category_segment,
        country_l1::varchar(200) as country_l1,
        regional_franchise_l2::varchar(200) as regional_franchise_l2,
        franchise_l3::varchar(200) as franchise_l3,
        brand_l4::varchar(200) as brand_l4,
        sub_category_l5::varchar(200) as sub_category_l5,
        platform_l6::varchar(200) as platform_l6,
        variance_l7::varchar(200) as variance_l7,
        pack_size_l8::varchar(200) as pack_size_l8,
        hashkey::varchar(200) as hashkey,
        effective_from::timestamp_ntz(9) as effective_from,
        effective_to::timestamp_ntz(9) as effective_to,
        active::varchar(2) as active,
        file_name::varchar(100) as file_name,
        run_id::number(14, 0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final