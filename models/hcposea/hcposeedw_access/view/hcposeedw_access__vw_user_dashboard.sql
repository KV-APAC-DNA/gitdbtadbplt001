with source as(
    select * from {{ ref('hcposeedw_integration__vw_user_dashboard') }}
),
final as(
    select 
        year as "year",
        month as "month",
        country as "country",
        sector as "sector",
        usr_name as "usr_name",
        last30_cnt as "last30_cnt",
        total_sales_manager as "total_sales_manager",
        total_msl as "total_msl",
        total_marketing as "total_marketing",
        total_others as "total_others",
        license_qty as "license_qty",
        wwid as "wwid",
        manager as "manager",
        profile_category as "profile_category",
        role as "role",
        company_name as "company_name",
        last_login_date as "last_login_date",
        active_flag as "active_flag",
        license_type as "license_type",
        last30_flag_sales_rep as "last30_flag_sales_rep",
        last30_flag_sales_manager as "last30_flag_sales_manager",
        last30_flag_total_msl as "last30_flag_total_msl",
        last30_flag_total_marketing as "last30_flag_total_marketing",
        last30_flag_total_others as "last30_flag_total_others",
        total_sales_rep as "total_sales_rep",
        monthly_active_login as "monthly_active_login",
        profile_name as "profile_name"
    from source
)
select * from final