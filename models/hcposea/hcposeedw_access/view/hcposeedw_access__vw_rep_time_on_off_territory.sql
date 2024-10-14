with source as(
    select * from {{ ref('hcposeedw_integration__vw_rep_time_on_off_territory') }}
),
final as(
    select 
        country as "country",
        time_on_off as "time_on_off",
        date as "date",
        year as "year",
        month as "month",
        quarter as "quarter",
        jnj_year as "jnj_year",
        jnj_month as "jnj_month",
        jnj_quarter as "jnj_quarter",
        my_year as "my_year",
        my_month as "my_month",
        my_quarter as "my_quarter",
        reason as "reason",
        hours_off as "hours_off",
        hours_on as "hours_on",
        working_days as "working_days",
        duration as "duration",
        l3_wwid as "l3_wwid",
        l3_username as "l3_username",
        l3_manager_name as "l3_manager_name",
        l2_wwid as "l2_wwid",
        l2_username as "l2_username",
        l2_manager_name as "l2_manager_name",
        l1_wwid as "l1_wwid",
        l1_username as "l1_username",
        l1_manager_name as "l1_manager_name",
        sales_rep_ntid as "sales_rep_ntid",
        sales_rep as "sales_rep",
        organization_l1_name as "organization_l1_name",
        organization_l2_name as "organization_l2_name",
        organization_l3_name as "organization_l3_name",
        organization_l4_name as "organization_l4_name",
        organization_l5_name as "organization_l5_name",
        flag as "flag"  
    from source
)
select * from final