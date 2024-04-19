with source as (
    select * from {{ ref('aspitg_integration__itg_sfmc_children_data') }}
),
final as (
    select 
        cntry_cd as "cntry_cd",
        parent_key as "parent_key",
        child_nm as "child_nm",
        child_birth_mnth as "child_birth_mnth",
        child_birth_year as "child_birth_year",
        child_gender as "child_gender",
        child_number as "child_number",
        file_name as "file_name",
        crtd_dttm as "crtd_dttm",
        updt_dttm as "updt_dttm"
    from source
)

select * from final 