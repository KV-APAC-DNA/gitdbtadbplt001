{{
    config(
        pre_hook="{{build_itg_pop6_product_lists_allocation()}}"
    )
}}
with sdl_pop6_hk_product_lists_allocation as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_product_lists_allocation') }}
),
sdl_pop6_kr_product_lists_allocation as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_product_lists_allocation') }}
),
sdl_pop6_tw_product_lists_allocation as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_product_lists_allocation') }}
),
sdl_pop6_jp_product_lists_allocation as
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_product_lists_allocation') }}
),
sdl_pop6_sg_product_lists_allocation as
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_product_lists_allocation') }}
),
sdl_pop6_th_product_lists_allocation as 
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_product_lists_allocation') }}
),
sdl as 
(
    SELECT 'HK' AS CNTRY_CD,
        product_group_status,
        product_group,
        product_list_status,
        product_list,
        null as product_list_code,
        pop_attribute_id,
        pop_attribute,
        pop_attribute_value_id,
        pop_attribute_value,
        prod_grp_date,
        file_name,
        run_id,
        crtd_dttm,
        hashkey
    FROM sdl_pop6_hk_product_lists_allocation SDL_HK
    UNION ALL
    SELECT 'KR' AS CNTRY_CD,
        product_group_status,
        product_group,
        product_list_status,
        product_list,
        null as product_list_code,
        pop_attribute_id,
        pop_attribute,
        pop_attribute_value_id,
        pop_attribute_value,
        prod_grp_date,
        file_name,
        run_id,
        crtd_dttm,
        hashkey
    FROM sdl_pop6_kr_product_lists_allocation SDL_KR
    UNION ALL
    SELECT 'TW' AS CNTRY_CD,
        product_group_status,
        product_group,
        product_list_status,
        product_list,
        null as product_list_code,
        pop_attribute_id,
        pop_attribute,
        pop_attribute_value_id,
        pop_attribute_value,
        prod_grp_date,
        file_name,
        run_id,
        crtd_dttm,
        hashkey
    FROM sdl_pop6_tw_product_lists_allocation SDL_TW
    UNION ALL
    SELECT 'JP' AS CNTRY_CD,
        product_group_status,
        product_group,
        product_list_status,
        product_list,
        product_list_code,
        pop_attribute_id,
        pop_attribute,
        pop_attribute_value_id,
        pop_attribute_value,
        prod_grp_date,
        file_name,
        run_id,
        crtd_dttm,
        hashkey
    FROM sdl_pop6_jp_product_lists_allocation SDL_JP
    UNION ALL
    SELECT 'SG' AS CNTRY_CD,
        product_group_status,
        product_group,
        product_list_status,
        product_list,
        product_list_code,
        pop_attribute_id,
        pop_attribute,
        pop_attribute_value_id,
        pop_attribute_value,
        prod_grp_date,
        file_name,
        run_id,
        crtd_dttm,
        hashkey
    FROM sdl_pop6_sg_product_lists_allocation SDL_SG
    UNION ALL
    SELECT 'TH' AS CNTRY_CD,
        product_group_status,
        product_group,
        product_list_status,
        product_list,
        product_list_code,
        pop_attribute_id,
        pop_attribute,
        pop_attribute_value_id,
        pop_attribute_value,
        prod_grp_date,
        file_name,
        run_id,
        crtd_dttm,
        hashkey
    FROM sdl_pop6_th_product_lists_allocation SDL_TH
),
wks as 
(
    SELECT 
        prod_grp.cntry_cd,
        prod_grp.src_file_date,
        prod_grp.product_group_status,
        prod_grp.product_group,
        prod_grp.product_list_status,
        prod_grp.product_list,
        prod_grp.POP_Attribute_ID,
        prod_grp.POP_Attribute,
        prod_grp.POP_Attribute_Value_ID,
        prod_grp.POP_Attribute_Value,
        prod_grp.prod_grp_date,
        prod_grp.hashkey,
        prod_grp.effective_from,
        CASE
            WHEN prod_grp.effective_to IS NULL THEN dateadd(DAY, -1, current_timestamp)
            ELSE prod_grp.effective_to
        END AS effective_to,
        'N' AS active,
        prod_grp.file_name,
        prod_grp.run_id,
        prod_grp.crtd_dttm,
        prod_grp.updt_dttm,
        prod_grp.product_list_code
    FROM 
        (
            SELECT itg.*
            FROM sdl,
                {{this}} itg
            WHERE sdl.hashkey != itg.hashkey
                AND sdl.product_group = itg.product_group
                AND sdl.product_list = itg.product_list
                AND nvl(sdl.POP_Attribute_ID, 'NA') = nvl(itg.POP_Attribute_ID, 'NA')
                AND nvl(sdl.POP_Attribute_Value_ID, 'NA') = nvl(itg.POP_Attribute_Value_ID, 'NA')
        ) prod_grp
    UNION ALL
    SELECT 
        prod_grp.cntry_cd,
        SUBSTRING(prod_grp.file_name, 1, 8) AS src_file_date,
        prod_grp.product_group_status,
        prod_grp.product_group,
        prod_grp.product_list_status,
        prod_grp.product_list,
        prod_grp.POP_Attribute_ID,
        prod_grp.POP_Attribute,
        prod_grp.POP_Attribute_Value_ID,
        prod_grp.POP_Attribute_Value,
        prod_grp.prod_grp_date,
        prod_grp.hashkey,
        current_timestamp AS effective_from,
        NULL AS effective_to,
        'Y' AS active,
        prod_grp.file_name,
        run_id,
        prod_grp.crtd_dttm,
        current_timestamp AS updt_dttm,
        prod_grp.product_list_code
    FROM 
        (
            SELECT sdl.*
            FROM sdl,
                {{this}} itg
            WHERE sdl.hashkey != itg.hashkey
                AND sdl.product_group = itg.product_group
                AND sdl.product_list = itg.product_list
                AND nvl(sdl.POP_Attribute_ID, 'NA') = nvl(itg.POP_Attribute_ID, 'NA')
                AND nvl(sdl.POP_Attribute_Value_ID, 'NA') = nvl(itg.POP_Attribute_Value_ID, 'NA')
                AND itg.active = 'Y'
        ) prod_grp
    UNION ALL
    SELECT 
        prod_grp.cntry_cd,
        SUBSTRING(prod_grp.file_name, 1, 8) AS src_file_date,
        prod_grp.product_group_status,
        prod_grp.product_group,
        prod_grp.product_list_status,
        prod_grp.product_list,
        prod_grp.POP_Attribute_ID,
        prod_grp.POP_Attribute,
        prod_grp.POP_Attribute_Value_ID,
        prod_grp.POP_Attribute_Value,
        prod_grp.prod_grp_date,
        prod_grp.hashkey,
        prod_grp.effective_from,
        NULL AS effective_to,
        'Y' AS active,
        prod_grp.file_name,
        run_id,
        prod_grp.crtd_dttm,
        current_timestamp AS updt_dttm,
        prod_grp.product_list_code
    FROM 
        (
            SELECT sdl.*,
                itg.effective_from
            FROM sdl,
                {{this}} itg
            WHERE sdl.hashkey = itg.hashkey
                AND sdl.product_group = itg.product_group
                AND sdl.product_list = itg.product_list
                AND nvl(sdl.POP_Attribute_ID, 'NA') = nvl(itg.POP_Attribute_ID, 'NA')
                AND nvl(sdl.POP_Attribute_Value_ID, 'NA') = nvl(itg.POP_Attribute_Value_ID, 'NA')
                AND itg.active = 'Y'
        ) prod_grp
    UNION ALL
    SELECT 
        prod_grp.cntry_cd,
        prod_grp.src_file_date,
        prod_grp.product_group_status,
        prod_grp.product_group,
        prod_grp.product_list_status,
        prod_grp.product_list,
        prod_grp.POP_Attribute_ID,
        prod_grp.POP_Attribute,
        prod_grp.POP_Attribute_Value_ID,
        prod_grp.POP_Attribute_Value,
        prod_grp.prod_grp_date,
        prod_grp.hashkey,
        prod_grp.effective_from,
        prod_grp.effective_to,
        prod_grp.active,
        prod_grp.file_name,
        prod_grp.run_id,
        prod_grp.crtd_dttm,
        prod_grp.updt_dttm,
        prod_grp.product_list_code
    FROM 
        (
            SELECT itg.*
            FROM sdl,
                {{this}} itg
            WHERE sdl.hashkey = itg.hashkey
                AND sdl.product_group = itg.product_group
                AND sdl.product_list = itg.product_list
                AND nvl(sdl.POP_Attribute_ID, 'NA') = nvl(itg.POP_Attribute_ID, 'NA')
                AND nvl(sdl.POP_Attribute_Value_ID, 'NA') = nvl(itg.POP_Attribute_Value_ID, 'NA')
                AND itg.active = 'N'
        ) prod_grp
    UNION ALL
    SELECT 
        prod_grp.cntry_cd,
        SUBSTRING(prod_grp.file_name, 1, 8) AS src_file_date,
        prod_grp.product_group_status,
        prod_grp.product_group,
        prod_grp.product_list_status,
        prod_grp.product_list,
        prod_grp.POP_Attribute_ID,
        prod_grp.POP_Attribute,
        prod_grp.POP_Attribute_Value_ID,
        prod_grp.POP_Attribute_Value,
        prod_grp.prod_grp_date,
        prod_grp.hashkey,
        current_timestamp AS effective_from,
        NULL AS effective_to,
        'Y' AS active,
        prod_grp.file_name,
        run_id,
        prod_grp.crtd_dttm,
        current_timestamp AS updt_dttm,
        prod_grp.product_list_code
    FROM (
            SELECT *
            FROM sdl
            WHERE product_group || product_list || nvl(POP_Attribute_ID, 'NA') || nvl(POP_Attribute_Value_ID, 'NA') NOT IN (
                    SELECT product_group || product_list || nvl(POP_Attribute_ID, 'NA') || nvl(POP_Attribute_Value_ID, 'NA')
                    FROM {{this}}
                )
        ) prod_grp
),
transformed as
(  
    SELECT * FROM wks
    UNION ALL
    SELECT * FROM {{this}}
    WHERE product_group || product_list || nvl(POP_Attribute_ID, 'NA') || nvl(POP_Attribute_Value_ID, 'NA') NOT IN 
        (
            SELECT product_group || product_list || nvl(POP_Attribute_ID, 'NA') || nvl(POP_Attribute_Value_ID, 'NA')
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
        product_list::varchar(100) as product_list,
        pop_attribute_id::varchar(255) as pop_attribute_id,
        pop_attribute::varchar(200) as pop_attribute,
        pop_attribute_value_id::varchar(255) as pop_attribute_value_id,
        pop_attribute_value::varchar(200) as pop_attribute_value,
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
    from transformed
)
select * from final