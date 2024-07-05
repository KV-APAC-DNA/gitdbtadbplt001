{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}
with sdl_pop6_hk_displays as (
    select * from {{ref('aspwks_integration__wks_pop6_hk_display')}}
),
sdl_pop6_kr_displays as (
    select * from {{ref('aspwks_integration__wks_pop6_kr_display')}}
),
sdl_pop6_tw_displays as (
    select * from {{ref('aspwks_integration__wks_pop6_tw_display')}}
),
sdl_pop6_jp_displays as (
    select * from {{ref('aspwks_integration__wks_pop6_jp_display')}}
),
sdl_pop6_sg_displays as (
    select * from {{ref('aspwks_integration__wks_pop6_sg_display')}}
),
sdl_pop6_th_displays as (
    select * from {{ref('aspwks_integration__wks_pop6_th_display')}}
),
transformed as (
    SELECT  
        'KR' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        display_plan_id as display_plan_id,
        display_type as display_type,
        display_code as display_code,
        display_name as display_name,
        start_date as start_date,
        end_date as end_date,
        checklist_method as checklist_method,
        display_number as display_number,
        product_attribute_id as product_attribute_id,
        product_attribute as product_attribute,
        product_attribute_value_id as product_attribute_value_id,
        product_attribute_value as product_attribute_value,
        comments as comments,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp() as updt_dttm
    FROM sdl_pop6_kr_displays
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='KR')
    {% endif %}
    union
    SELECT  
        'TW' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        display_plan_id as display_plan_id,
        display_type as display_type,
        display_code as display_code,
        display_name as display_name,
        start_date as start_date,
        end_date as end_date,
        checklist_method as checklist_method,
        display_number as display_number,
        product_attribute_id as product_attribute_id,
        product_attribute as product_attribute,
        product_attribute_value_id as product_attribute_value_id,
        product_attribute_value as product_attribute_value,
        comments as comments,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp() as updt_dttm
    FROM sdl_pop6_tw_displays
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='TW')
    {% endif %}
    union
    SELECT  
        'HK' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        display_plan_id as display_plan_id,
        display_type as display_type,
        display_code as display_code,
        display_name as display_name,
        start_date as start_date,
        end_date as end_date,
        checklist_method as checklist_method,
        display_number as display_number,
        product_attribute_id as product_attribute_id,
        product_attribute as product_attribute,
        product_attribute_value_id as product_attribute_value_id,
        product_attribute_value as product_attribute_value,
        comments as comments,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp() as updt_dttm
    FROM sdl_pop6_hk_displays
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='HK')
    {% endif %}
    union
    SELECT  
        'JP' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        display_plan_id as display_plan_id,
        display_type as display_type,
        display_code as display_code,
        display_name as display_name,
        start_date as start_date,
        end_date as end_date,
        checklist_method as checklist_method,
        display_number as display_number,
        product_attribute_id as product_attribute_id,
        product_attribute as product_attribute,
        product_attribute_value_id as product_attribute_value_id,
        product_attribute_value as product_attribute_value,
        comments as comments,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp() as updt_dttm
    FROM sdl_pop6_jp_displays
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='JP')
    {% endif %}
    union
    SELECT  
        'SG' as cntry_cd,
        substring(file_name, 1, 8) as src_file_date,
        visit_id as visit_id,
        display_plan_id as display_plan_id,
        display_type as display_type,
        display_code as display_code,
        display_name as display_name,
        start_date as start_date,
        end_date as end_date,
        checklist_method as checklist_method,
        display_number as display_number,
        product_attribute_id as product_attribute_id,
        product_attribute as product_attribute,
        product_attribute_value_id as product_attribute_value_id,
        product_attribute_value as product_attribute_value,
        comments as comments,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp() as updt_dttm
    FROM sdl_pop6_sg_displays
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='SG')
    {% endif %}
    union
    SELECT 
        'TH' as cntry_cd,
        substring(file_name,1,8) as src_file_date,
        visit_id as visit_id,
        display_plan_id as display_plan_id,
        display_type as display_type,
        display_code as display_code,
        display_name as display_name,
        start_date as start_date,
        end_date as end_date,
        checklist_method as checklist_method,
        display_number as display_number,
        product_attribute_id as product_attribute_id,
        product_attribute as product_attribute,
        product_attribute_value_id as product_attribute_value_id,
        product_attribute_value as product_attribute_value,
        comments as comments,
        field_id as field_id,
        field_code as field_code,
        field_label as field_label,
        field_type as field_type,
        dependent_on_field_id as dependent_on_field_id,
        response as response,
        file_name as file_name,
        run_id as run_id,
        crtd_dttm as crtd_dttm,
        current_timestamp() as updt_dttm
    FROM sdl_pop6_th_displays
    {% if is_incremental() %}
        where file_name not in (select distinct file_name from {{this}} where cntry_cd='TH')
    {% endif %}
),
final as(
    select
        cntry_cd::varchar(10) as cntry_cd,
        src_file_date::varchar(10) as src_file_date,
        visit_id::varchar(255) as visit_id,
        display_plan_id::varchar(255) as display_plan_id,
        display_type::varchar(255) as display_type,
        display_code::varchar(255) as display_code,
        display_name::varchar(255) as display_name,
        start_date::date as start_date,
        end_date::date as end_date,
        checklist_method::varchar(50) as checklist_method,
        display_number::number(18,0) as display_number,
        product_attribute_id::varchar(255) as product_attribute_id,
        product_attribute::varchar(200) as product_attribute,
        product_attribute_value_id::varchar(255) as product_attribute_value_id,
        product_attribute_value::varchar(255) as product_attribute_value,
        comments::varchar(255) as comments,
        field_id::varchar(255) as field_id,
        field_code::varchar(255) as field_code,
        field_label::varchar(255) as field_label,
        field_type::varchar(50) as field_type,
        dependent_on_field_id::varchar(255) as dependent_on_field_id,
        response::varchar(65535) as response,
        file_name::varchar(100) as file_name,
        run_id::number(14,0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
	    updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final