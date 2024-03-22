with source as (
    select * from {{ ref('thaitg_integration__itg_th_one_jnj_data') }}
),
final as (
    select
        name "name",
        code as "code",
        changetrackingmask as "changetrackingmask",
        date_level as "date_level",
        date_value as "date_value",
        sector_function as "sector_function",
        functional_area as "functional_area",
        subject as "subject",
        kpi as "kpi",
        level_1_def as "level_1_def",
        level_1 as "level_1",
        level_2_def as "level_2_def",
        level_2 as "level_2",
        ref_value as "ref_value",
        actual_value as "actual_value",
        add_info as "add_info",
        default_view_flag as "default_view_flag",
        compliance_definition as "compliance_definition",
        crtd_dttm as "crtd_dttm",
        updt_dttm as "updt_dttm"
    from source
)
select * from final