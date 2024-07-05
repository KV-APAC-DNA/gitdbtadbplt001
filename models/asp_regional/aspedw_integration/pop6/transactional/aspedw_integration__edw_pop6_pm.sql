with edw_vw_pop6_visits_display as (
    select * from {{ ref('aspedw_integration__edw_vw_pop6_visits_display') }}
),
edw_vw_pop6_store as (
    select * from {{ ref('aspedw_integration__edw_vw_pop6_store') }}
),
edw_vw_pop6_salesperson as (
    select * from {{ ref('aspedw_integration__edw_vw_pop6_salesperson') }}
),
edw_vw_pop6_visits_prod_attribute_audits as (
    select * from {{ ref('aspedw_integration__edw_vw_pop6_visits_prod_attribute_audits') }}
),
edw_vw_pop6_tasks as (
    select * from {{ ref('aspedw_integration__edw_vw_pop6_tasks') }}
),
edw_vw_pop6_visits_sku_audits as (
    select * from {{ ref('aspedw_integration__edw_vw_pop6_visits_sku_audits') }}
),
edw_vw_pop6_products as (
    select * from {{ ref('aspedw_integration__edw_vw_pop6_products') }}
),
itg_photo_mgmnt_url as (
    select * from {{ ref('ntaitg_integration__itg_photo_mgmnt_url') }}
),
itg_pop6_executed_visits as (
    select * from {{ ref('aspitg_integration__itg_pop6_executed_visits') }}
),
itg_pop6_promotions as (
    select * from {{ ref('aspitg_integration__itg_pop6_promotions') }}
),
itg_pop6_general_audits as (
    select * from {{ ref('aspitg_integration__itg_pop6_general_audits') }}
),
display as (
    SELECT 'display' AS data_type,
        disp.visit_id AS taskid,
        photo.filename AS filename,
        'http://itx-arm-conapdna-aspac-prod.s3.amazonaws.com/rex/cdl/img/' AS PATH,
        disp.product_attribute_value AS BRAND,
        disp.visit_date AS mrchr_visitdate,
        store.pop_name AS customername,
        store.sales_group_name AS salesgroup,
        store.retail_environment_ps AS storetype,
        store.channel AS dist_chnl,
        store.country AS country,
        disp.display_name AS salescyclename,
        disp.comments AS salescampaignname,
        disp.field_code,
        disp.field_label,
        rep.first_name AS salesperson_firstname,
        rep.last_name AS salesperson_lastname,
        disp.popdb_id AS customerid,
        disp.pop_code AS remotekey,
        NULL AS secondarytradecode,
        NULL AS secondarytradename
    FROM edw_vw_pop6_visits_display disp,
        (
            select distinct photo_key || '.png' as filename,
                response
            from itg_photo_mgmnt_url a
        ) photo,
        edw_vw_pop6_store store,
        edw_vw_pop6_salesperson rep
    WHERE UPPER(disp.field_type) LIKE 'PHOTO%'
        AND SPLIT_PART(filename, '_', 1) = 'display'
        AND visit_id || display_plan_id || field_id || product_attribute_value_id = SPLIT_PART(filename, '_', 2) || SPLIT_PART(filename, '_', 3) || SPLIT_PART(filename, '_', 4) || SPLIT_PART(filename, '_', 5)
        AND disp.pop_code = store.pop_code(+)
        AND disp.username = rep.username(+)
),
product as (
    SELECT 'product' as data_type,
        prod.visit_id AS taskid,
        photo.filename AS filename,
        'http://itx-arm-conapdna-aspac-prod.s3.amazonaws.com/rex/cdl/img/' AS PATH,
        prod.product_attribute_value AS BRAND,
        prod.visit_date AS mrchr_visitdate,
        store.pop_name AS customername,
        store.sales_group_name AS salesgroup,
        store.retail_environment_ps AS storetype,
        store.channel AS dist_chnl,
        store.country AS country,
        prod.audit_form AS salescyclename,
        prod.section AS salescampaignname,
        prod.field_code,
        prod.field_label,
        rep.first_name AS salesperson_firstname,
        rep.last_name AS salesperson_lastname,
        prod.popdb_id AS customerid,
        prod.pop_code AS remotekey,
        NULL AS secondarytradecode,
        NULL AS secondarytradename
    FROM edw_vw_pop6_visits_prod_attribute_audits prod,
        (
            select distinct photo_key || '.png' as filename,
                response
            from itg_photo_mgmnt_url a
        ) photo,
        edw_vw_pop6_store store,
        edw_vw_pop6_salesperson rep
    WHERE UPPER(prod.field_type) LIKE 'PHOTO%'
        AND SPLIT_PART(filename, '_', 1) = 'product'
        AND visit_id || audit_form_id || field_id || product_attribute_value_id = SPLIT_PART(filename, '_', 2) || SPLIT_PART(filename, '_', 3) || SPLIT_PART(filename, '_', 4) || SPLIT_PART(filename, '_', 5)
        AND prod.pop_code = store.pop_code(+)
        AND prod.username = rep.username(+)
),
tasks as (
    SELECT 'tasks' as data_type,
        tasks.visit_id AS taskid,
        photo.filename AS filename,
        'http://itx-arm-conapdna-aspac-prod.s3.amazonaws.com/rex/cdl/img/' AS PATH,
        NULL AS BRAND,
        tasks.visit_date AS mrchr_visitdate,
        store.pop_name AS customername,
        store.sales_group_name AS salesgroup,
        store.retail_environment_ps AS storetype,
        store.channel AS dist_chnl,
        store.country AS country,
        tasks.task_group AS salescyclename,
        tasks.task_name AS salescampaignname,
        tasks.field_code,
        tasks.field_label,
        rep.first_name AS salesperson_firstname,
        rep.last_name AS salesperson_lastname,
        tasks.popdb_id AS customerid,
        tasks.pop_code AS remotekey,
        NULL AS secondarytradecode,
        NULL AS secondarytradename
    FROM (
            SELECT tasks.cntry_cd,
                tasks.visit_id,
                tasks.task_group,
                tasks.task_id,
                tasks.task_name,
                tasks.field_id,
                tasks.field_code,
                tasks.field_label,
                tasks.field_type,
                tasks.dependent_on_field_id,
                tasks.response,
                tasks.address,
                tasks.check_in_longitude,
                tasks.check_in_latitude,
                tasks.check_out_longitude,
                tasks.check_out_latitude,
                tasks.check_in_photo,
                tasks.check_out_photo,
                tasks.user_full_name,
                tasks.superior_username,
                tasks.superior_name,
                tasks.planned_visit,
                tasks.cancelled_visit,
                tasks.cancellation_reason,
                tasks.cancellation_note,
                visit.visit_date,
                visit.check_in_datetime,
                visit.check_out_datetime,
                visit.popdb_id,
                visit.pop_code,
                visit.pop_name,
                visit.username
            FROM edw_vw_pop6_tasks tasks,
                (
                    select visit.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY visit_id,
                            popdb_id
                            ORDER BY run_id DESC
                        ) rn
                    from itg_pop6_executed_visits visit
                ) visit
            WHERE UPPER(tasks.field_type) LIKE 'PHOTO%'
                AND visit.visit_id = tasks.visit_id
                AND visit.rn = '1'
        ) tasks,
        (
            select distinct photo_key || '.png' as filename,
                response
            from itg_photo_mgmnt_url a
        ) photo,
        edw_vw_pop6_store store,
        edw_vw_pop6_salesperson rep
    WHERE SPLIT_PART(filename, '_', 1) = 'tasks'
        AND visit_id || task_id || field_id = SPLIT_PART(filename, '_', 2) || SPLIT_PART(filename, '_', 3) || SPLIT_PART(filename, '_', 4)
        AND tasks.pop_code = store.pop_code(+)
        AND tasks.username = rep.username(+)
),
promotions as (
    SELECT 'promotions' as data_type,
        promo.visit_id AS taskid,
        photo.filename AS filename,
        'http://itx-arm-conapdna-aspac-prod.s3.amazonaws.com/rex/cdl/img/' AS PATH,
        NULL AS BRAND,
        promo.visit_date AS mrchr_visitdate,
        store.pop_name AS customername,
        store.sales_group_name AS salesgroup,
        store.retail_environment_ps AS storetype,
        store.channel AS dist_chnl,
        store.country AS country,
        promo.promotion_type AS salescyclename,
        promo.promotion_name AS salescampaignname,
        promo.promotion_mechanics AS field_code,
        NULL AS field_label,
        rep.first_name AS salesperson_firstname,
        rep.last_name AS salesperson_lastname,
        promo.popdb_id AS customerid,
        promo.pop_code AS remotekey,
        NULL AS secondarytradecode,
        NULL AS secondarytradename
    FROM (
            SELECT promo.cntry_cd,
                promo.src_file_date,
                promo.visit_id,
                promo.promotion_plan_id,
                promo.promotion_code,
                promo.promotion_name,
                promo.promotion_mechanics,
                promo.promotion_type,
                promo.start_date,
                promo.end_date,
                promo.product_attribute_id,
                promo.product_attribute,
                promo.product_attribute_value_id,
                promo.product_attribute_value,
                promo.promotion_price,
                promo.promotion_compliance,
                promo.actual_price,
                promo.non_compliance_reason,
                promo.photo,
                visit.visit_date,
                visit.check_in_datetime,
                visit.check_out_datetime,
                visit.popdb_id,
                visit.pop_code,
                visit.pop_name,
                visit.username
            FROM (
                    select promo.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY promotion_plan_id
                            ORDER BY run_id DESC
                        ) rn
                    from itg_pop6_promotions promo
                ) promo,
                (
                    select visit.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY visit_id,
                            popdb_id
                            ORDER BY run_id DESC
                        ) rn
                    from itg_pop6_executed_visits visit
                ) visit
            WHERE visit.visit_id = promo.visit_id
                and promo.rn = '1'
                and visit.rn = '1'
        ) promo,
        (
            select distinct photo_key || '.png' as filename,
                response
            from itg_photo_mgmnt_url a
        ) photo,
        edw_vw_pop6_store store,
        edw_vw_pop6_salesperson rep
    WHERE SPLIT_PART(filename, '_', 1) = 'promotions'
        AND promotion_plan_id = SPLIT_PART(filename, '_', 2)
        AND promo.pop_code = store.pop_code(+)
        AND promo.username = rep.username(+)
),
general as (
    SELECT 'general' as data_type,
        general.visit_id AS taskid,
        photo.filename AS filename,
        'http://itx-arm-conapdna-aspac-prod.s3.amazonaws.com/rex/cdl/img/' AS PATH,
        NULL AS BRAND,
        general.visit_date AS mrchr_visitdate,
        store.pop_name AS customername,
        store.sales_group_name AS salesgroup,
        store.retail_environment_ps AS storetype,
        store.channel AS dist_chnl,
        store.country AS country,
        general.audit_form AS salescyclename,
        general.section AS salescampaignname,
        general.field_code,
        general.field_label,
        rep.first_name AS salesperson_firstname,
        rep.last_name AS salesperson_lastname,
        general.popdb_id AS customerid,
        general.pop_code AS remotekey,
        NULL AS secondarytradecode,
        NULL AS secondarytradename
    FROM (
            SELECT general.cntry_cd,
                general.cntry_cd,
                general.src_file_date,
                general.visit_id,
                general.audit_form_id,
                general.audit_form,
                general.section_id,
                general.section,
                general.subsection_id,
                general.subsection,
                general.field_id,
                general.field_code,
                general.field_label,
                general.field_type,
                general.dependent_on_field_id,
                general.response,
                visit.visit_date,
                visit.check_in_datetime,
                visit.check_out_datetime,
                visit.popdb_id,
                visit.pop_code,
                visit.pop_name,
                visit.username
            FROM (
                    select general.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY visit_id,
                            audit_form_id,
                            section_id
                            ORDER BY run_id DESC
                        ) rn
                    from itg_pop6_general_audits general
                ) general,
                (
                    select visit.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY visit_id,
                            popdb_id
                            ORDER BY run_id DESC
                        ) rn
                    from itg_pop6_executed_visits visit
                ) visit
            WHERE UPPER(general.field_type) LIKE 'PHOTO%'
                AND visit.visit_id = general.visit_id
                and general.rn = '1'
                and visit.rn = '1'
        ) general,
        (
            select distinct photo_key || '.png' as filename,
                response
            from itg_photo_mgmnt_url a
        ) photo,
        edw_vw_pop6_store store,
        edw_vw_pop6_salesperson rep
    WHERE SPLIT_PART(filename, '_', 1) = 'general'
        AND visit_id || audit_form_id || section_id = SPLIT_PART(filename, '_', 3) || SPLIT_PART(filename, '_', 4) || SPLIT_PART(filename, '_', 5)
        AND general.pop_code = store.pop_code(+)
        AND general.username = rep.username(+)
),
sku as (
    SELECT 'sku' as data_type,
        sku.visit_id AS taskid,
        photo.filename AS filename,
        'http://itx-arm-conapdna-aspac-prod.s3.amazonaws.com/rex/cdl/img/' AS PATH,
        prod.brand_l4 AS BRAND,
        sku.visit_date AS mrchr_visitdate,
        store.pop_name AS customername,
        store.sales_group_name AS salesgroup,
        store.retail_environment_ps AS storetype,
        store.channel AS dist_chnl,
        store.country AS country,
        sku.audit_form AS salescyclename,
        sku.section AS salescampaignname,
        sku.field_code,
        sku.sku AS field_label,
        rep.first_name AS salesperson_firstname,
        rep.last_name AS salesperson_lastname,
        sku.popdb_id AS customerid,
        sku.pop_code AS remotekey,
        NULL AS secondarytradecode,
        NULL AS secondarytradename
    FROM edw_vw_pop6_visits_sku_audits sku,
        (
            select distinct photo_key || '.png' as filename,
                response
            from itg_photo_mgmnt_url a
        ) photo,
        edw_vw_pop6_store store,
        edw_vw_pop6_salesperson rep,
        edw_vw_pop6_products prod
    WHERE UPPER(sku.field_type) LIKE 'PHOTO%'
        AND SPLIT_PART(filename, '_', 1) = 'sku'
        AND visit_id || audit_form_id || sku_id = SPLIT_PART(filename, '_', 2) || SPLIT_PART(filename, '_', 3) || SPLIT_PART(filename, '_', 4)
        AND sku.pop_code = store.pop_code(+)
        AND sku.username = rep.username(+)
        and sku.sku_id = prod.productdb_id(+)
),
transformed as (
    select * from display
    union all
    select * from product
    union all
    select * from tasks
    union all
    select * from promotions
    union all
    select * from general
    union all
    select * from sku
),
final as (
    select
        data_type::varchar(10) as data_type,
        taskid::varchar(255) as taskid,
        filename::varchar(1057) as filename,
        path::varchar(64) as path,
        brand::varchar(255) as brand,
        mrchr_visitdate::date as mrchr_visitdate,
        customername::varchar(100) as customername,
        salesgroup::varchar(200) as salesgroup,
        storetype::varchar(200) as storetype,
        dist_chnl::varchar(200) as dist_chnl,
        country::varchar(200) as country,
        salescyclename::varchar(255) as salescyclename,
        salescampaignname::varchar(255) as salescampaignname,
        field_code::varchar(255) as field_code,
        field_label::varchar(255) as field_label,
        salesperson_firstname::varchar(50) as salesperson_firstname,
        salesperson_lastname::varchar(50) as salesperson_lastname,
        customerid::varchar(255) as customerid,
        remotekey::varchar(50) as remotekey,
        secondarytradecode::varchar(1) as secondarytradecode,
        secondarytradename::varchar(1) as secondarytradename
    from transformed
)
select * from final