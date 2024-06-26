{{
    config(
        pre_hook='{{build_itg_pop6_product_lists_products_temp()}}'
    )
}}
with sdl_pop6_hk_product_lists_products as (
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_product_lists_products') }}
),
sdl_pop6_kr_product_lists_products as (
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_product_lists_products') }}
),
sdl_pop6_tw_product_lists_products as (
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_product_lists_products') }}
),
sdl_pop6_jp_product_lists_products as (
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_product_lists_products') }}
),
sdl_pop6_sg_product_lists_products as (
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_product_lists_products') }}
),
sdl_pop6_th_product_lists_products as (
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_product_lists_products') }}
),
itg_pop6_product_lists_products as (
    -- select * from {{ source('ntaitg_integration','itg_pop6_product_lists_products_temp') }}
    select * from ntaitg_integration__itg_pop6_product_lists_products_temp
),
sdl as (
    SELECT 'HK' AS CNTRY_CD,
        product_list,
        null as product_list_code,
        productdb_id,
        sku,
        msl_ranking,
        prod_grp_date,
        file_name,
        run_id,
        crtd_dttm,
        hashkey
    FROM sdl_pop6_hk_product_lists_products SDL_HK
    UNION ALL
    SELECT 'KR' AS CNTRY_CD,
        product_list,
        null as product_list_code,
        productdb_id,
        sku,
        msl_ranking,
        prod_grp_date,
        file_name,
        run_id,
        crtd_dttm,
        hashkey
    FROM sdl_pop6_kr_product_lists_products SDL_KR
    UNION ALL
    SELECT 'TW' AS CNTRY_CD,
        product_list,
        null as product_list_code,
        productdb_id,
        sku,
        msl_ranking,
        prod_grp_date,
        file_name,
        run_id,
        crtd_dttm,
        hashkey
    FROM sdl_pop6_tw_product_lists_products SDL_TW
    UNION ALL
    SELECT 'JP' AS CNTRY_CD,
        product_list,
        product_list_code,
        productdb_id,
        sku,
        msl_ranking,
        prod_grp_date,
        file_name,
        run_id,
        crtd_dttm,
        hashkey
    FROM sdl_pop6_jp_product_lists_products SDL_JP
    UNION ALL
    SELECT 'SG' AS CNTRY_CD,
        product_list,
        product_list_code,
        productdb_id,
        sku,
        msl_ranking,
        prod_grp_date,
        file_name,
        run_id,
        crtd_dttm,
        hashkey
    FROM sdl_pop6_sg_product_lists_products SDL_SG
    UNION ALL
    SELECT 'TH' AS CNTRY_CD,
        product_list,
        product_list_code,
        productdb_id,
        sku,
        msl_ranking,
        prod_grp_date,
        file_name,
        run_id,
        crtd_dttm,
        hashkey
    FROM sdl_pop6_th_product_lists_products SDL_TH
),
wks as (
    SELECT poplist.cntry_cd,
        poplist.src_file_date,
        poplist.product_list,
        poplist.productdb_id,
        poplist.sku,
        poplist.msl_ranking,
        poplist.prod_grp_date,
        poplist.hashkey,
        poplist.effective_from,
        CASE
            WHEN poplist.effective_to IS NULL THEN dateadd(DAY, -1, current_timestamp())
            ELSE poplist.effective_to
        END AS effective_to,
        'N' AS active,
        poplist.file_name,
        poplist.run_id,
        poplist.crtd_dttm,
        poplist.updt_dttm,
        poplist.product_list_code
    FROM (
            SELECT itg.*
            FROM sdl,
                itg_pop6_product_lists_products itg
            WHERE sdl.hashkey != itg.hashkey
                AND sdl.productdb_id = itg.productdb_id
                AND sdl.product_list = itg.product_list
        ) poplist
    UNION ALL
    SELECT poplist.cntry_cd,
        SUBSTRING(poplist.file_name, 1, 8) AS src_file_date,
        poplist.product_list,
        poplist.productdb_id,
        poplist.sku,
        poplist.msl_ranking,
        poplist.prod_grp_date,
        poplist.hashkey,
        current_timestamp() AS effective_from,
        NULL AS effective_to,
        'Y' AS active,
        poplist.file_name,
        poplist.run_id,
        poplist.crtd_dttm,
        current_timestamp() AS updt_dttm,
        poplist.product_list_code
    FROM (
            SELECT sdl.*
            FROM sdl,
                itg_pop6_product_lists_products itg
            WHERE sdl.hashkey != itg.hashkey
                AND sdl.productdb_id = itg.productdb_id
                AND sdl.product_list = itg.product_list
                AND itg.active = 'Y'
        ) poplist
    UNION ALL
    SELECT poplist.cntry_cd,
        SUBSTRING(poplist.file_name, 1, 8) AS src_file_date,
        poplist.product_list,
        poplist.productdb_id,
        poplist.sku,
        poplist.msl_ranking,
        poplist.prod_grp_date,
        poplist.hashkey,
        poplist.effective_from,
        NULL AS effective_to,
        'Y' AS active,
        poplist.file_name,
        poplist.run_id,
        poplist.crtd_dttm,
        current_timestamp() AS updt_dttm,
        poplist.product_list_code
    FROM (
            SELECT sdl.*,
                itg.effective_from
            FROM sdl,
                itg_pop6_product_lists_products itg
            WHERE sdl.hashkey = itg.hashkey
                AND sdl.productdb_id = itg.productdb_id
                AND sdl.product_list = itg.product_list
                AND itg.active = 'Y'
        ) poplist
    UNION ALL
    SELECT poplist.cntry_cd,
        poplist.src_file_date,
        poplist.product_list,
        poplist.productdb_id,
        poplist.sku,
        poplist.msl_ranking,
        poplist.prod_grp_date,
        poplist.hashkey,
        poplist.effective_from,
        poplist.effective_to,
        poplist.active,
        poplist.file_name,
        poplist.run_id,
        poplist.crtd_dttm,
        poplist.updt_dttm,
        poplist.product_list_code
    FROM (
            SELECT itg.*
            FROM sdl,
                itg_pop6_product_lists_products itg
            WHERE sdl.hashkey = itg.hashkey
                AND sdl.productdb_id = itg.productdb_id
                AND sdl.product_list = itg.product_list
                AND itg.active = 'N'
        ) poplist
    UNION ALL
    SELECT poplist.cntry_cd,
        SUBSTRING(poplist.file_name, 1, 8) AS src_file_date,
        poplist.product_list,
        poplist.productdb_id,
        poplist.sku,
        poplist.msl_ranking,
        poplist.prod_grp_date,
        poplist.hashkey,
        current_timestamp() AS effective_from,
        NULL AS effective_to,
        'Y' AS active,
        poplist.file_name,
        poplist.run_id,
        poplist.crtd_dttm,
        current_timestamp() AS updt_dttm,
        poplist.product_list_code
    FROM (
            SELECT *
            FROM sdl
            WHERE productdb_id || product_list NOT IN (
                    SELECT productdb_id || product_list
                    FROM itg_pop6_product_lists_products
                )
        ) poplist
),
itg_pop6_product_lists_products_wrk as (
    SELECT * FROM wks
    UNION
    SELECT * FROM itg_pop6_product_lists_products
    WHERE productdb_id || product_list NOT IN (
            SELECT productdb_id || product_list
            FROM wks
        )
),
final as (
    select 
        cntry_cd::varchar(10) as cntry_cd,
        src_file_date::varchar(10) as src_file_date,
        product_list::varchar(100) as product_list,
        productdb_id::varchar(255) as productdb_id,
        sku::varchar(150) as sku,
        msl_ranking::varchar(20) as msl_ranking,
        prod_grp_date::date as prod_grp_date,
        hashkey::varchar(200) as hashkey,
        effective_from::timestamp_ntz(9) as effective_from,
        effective_to::timestamp_ntz(9) as effective_to,
        active::varchar(2) as active,
        file_name::varchar(100) as file_name,
        run_id::number(14,0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        product_list_code::varchar(100) as product_list_code
    from itg_pop6_product_lists_products_wrk
)
select * from final