with source as (
    select * from dev_dna_core.snaposeitg_integration.itg_sfmc_children_data 
    --ref('aspitg_integration__itg_sfmc_children_data')
),
final as (
    select 
        cntry_cd as country_code,
        parent_key,
        child_nm as child_name,
        child_birth_mnth as child_birth_month,
        child_birth_year,
        child_gender,
        child_number
    from source
)
select * from final