with source as (
    select * from {{ ref('aspitg_integration__itg_pop6_pops') }}
),
final as (
    select cntry_cd,
        src_file_date,
        status,
        popdb_id,
        pop_code,
        pop_name,
        address,
        longitude,
        latitude,
        country,
        channel,
        retail_environment_ps,
        customer,
        sales_group_code,
        sales_group_name,
        customer_grade,
        external_pop_code,
        business_units_id,
        business_unit_name,
        territory_or_region
    from source
    where active = 'Y'
)
select * from final