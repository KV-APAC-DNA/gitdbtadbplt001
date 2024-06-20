with itg_pop6_displays as (
    select * from ntaitg_integration.itg_pop6_displays
)
itg_pop6_product_attribute_audits as (
    select * from ntaitg_integration.itg_pop6_product_attribute_audits
),
itg_pop6_tasks as (
    select * from ntaitg_integration.itg_pop6_tasks
),
itg_pop6_general_audits as (
    select * from ntaitg_integration.itg_pop6_general_audits
),
itg_pop6_promotions as (
    select * from ntaitg_integration.itg_pop6_promotions
),
itg_pop6_sku_audits as (
    select * from ntaitg_integration.itg_pop6_sku_audits
),
itg_photo_mgmnt_url as (
    select * from ntaitg_integration.itg_photo_mgmnt_url
    -- select * from {{ source('ntaitg_integration', 'itg_photo_mgmnt_url_temp')}}
),
displays as (
    SELECT DISTINCT 'display' || '_' || visit_id || '_' || display_plan_id || '_' || field_id || '_' || product_attribute_value_id AS PHOTO_KEY,
        response,
        regexp_count(response, '[jpg]{3}') url_cnt,
        run_id,
        ROW_NUMBER() OVER (
            PARTITION BY visit_id,
            display_plan_id,
            field_id,
            product_attribute_value_id
            ORDER BY run_id DESC
        ) rn
    FROM itg_pop6_displays
    WHERE UPPER(field_type) LIKE 'PHOTO%'
        AND regexp_count(response, '[jpg]{3}') > 0
),
attribute_audits as (
    SELECT DISTINCT 'product' || '_' || visit_id || '_' || audit_form_id || '_' || field_id || '_' || product_attribute_value_id AS PHOTO_KEY,
        response,
        regexp_count(response, '[jpg]{3}') url_cnt,
        run_id,
        ROW_NUMBER() OVER (
            PARTITION BY visit_id,
            audit_form_id,
            field_id,
            product_attribute_value_id
            ORDER BY run_id DESC
        ) rn
    FROM itg_pop6_product_attribute_audits
    WHERE UPPER(field_type) LIKE 'PHOTO%'
        AND regexp_count(response, '[jpg]{3}') > 0
),
tasks as (
    SELECT DISTINCT 'tasks' || '_' || visit_id || '_' || task_id || '_' || field_id AS PHOTO_KEY,
        response,
        regexp_count(response, '[jpg]{3}') url_cnt,
        run_id,
        ROW_NUMBER() OVER (
            PARTITION BY visit_id,
            task_id,
            field_id
            ORDER BY run_id DESC
        ) rn
    FROM itg_pop6_tasks
    WHERE UPPER(field_type) LIKE 'PHOTO%'
        AND regexp_count(response, '[jpg]{3}') > 0
),
general_audits as (
    SELECT DISTINCT 'general_audit' || '_' || visit_id || '_' || audit_form_id || '_' || section_id AS PHOTO_KEY,
        response,
        regexp_count(response, '[jpg]{3}') url_cnt,
        run_id,
        ROW_NUMBER() OVER (
            PARTITION BY visit_id,
            audit_form_id,
            section_id
            ORDER BY run_id DESC
        ) rn
    FROM itg_pop6_general_audits
    WHERE UPPER(field_type) LIKE 'PHOTO%'
        AND regexp_count(response, '[jpg]{3}') > 0
),
promotions as (
    SELECT DISTINCT 'promotions' || '_' || promotion_plan_id AS PHOTO_KEY,
        photo,
        regexp_count(photo, '[jpg]{3}') url_cnt,
        run_id,
        ROW_NUMBER() OVER (
            PARTITION BY promotion_plan_id
            ORDER BY run_id DESC
        ) rn
    FROM itg_pop6_promotions
    WHERE regexp_count(photo, '[jpg]{3}') > 0
),
audits as (
    SELECT DISTINCT 'sku' || '_' || visit_id || '_' || audit_form_id || '_' || sku_id AS PHOTO_KEY,
        response,
        regexp_count(response, '[jpg]{3}') url_cnt,
        run_id,
        ROW_NUMBER() OVER (
            PARTITION BY visit_id,
            audit_form_id,
            sku_id
            ORDER BY run_id DESC
        ) rn
    FROM itg_pop6_sku_audits
    WHERE UPPER(field_type) LIKE 'PHOTO%'
        AND regexp_count(response, '[jpg]{3}') > 0
),
src as (
    select * from displays
    union all
    select * from attribute_audits
    union all
    select * from tasks
    union all
    select * from general_audits
    union all
    select * from promotions
    union all
    select * from audits
),
tgt as (
    select distinct original_photo_key,
        create_dt
    from itg_photo_mgmnt_url
),
final as (
    select src.photo_key,
        src.response,
        src.url_cnt,
        src.run_id,
        case
            when tgt.create_dt is null then 'I'
            else 'U'
        end change_flag
    from src, tgt 
    where rn = 1
        and src.photo_key = tgt.original_photo_key(+)
)
select * from final