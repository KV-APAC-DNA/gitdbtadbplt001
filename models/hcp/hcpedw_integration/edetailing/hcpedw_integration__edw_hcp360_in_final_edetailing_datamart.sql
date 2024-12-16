with
detailing_va_hcp_jn as (
    select * from   {{ ref('hcpitg_integration__itg_hcp360_in_detailing_va_hcp_join') }}
),

region_filter_hcp as (
    select *
    from {{ ref('hcpitg_integration__itg_hcp360_in_region_filter_hcp') }}
),

dcr_hcp_region_join as (
    select *
    from {{ ref('hcpitg_integration__itg_hcp360_in_dcr_hcp_region_join') }}
),



final_edetailing_datamart as (
    select

        va_name,
        page_name,
        team as team_name,
        month,
        rbm,
        zbm,
        fbm,
        msr,
        hq_code as hq,
        msr_name,
        dsg,
        customer_name,
        cid,
        speciality,
        customer_type,
        active,
        dcr_date,
        page_start_time,
        page_end_time,
        seconds,
        capped_seconds,
        as_on,
        franchise,
        sub_group,
        group_name,
        brand,
        v_custid,
        cust_spec,
        core_noncore,
        calls_planned,
        is_active,
        dcr_month,
        dcr_year,
        v_terrid,
        null as distinct_hcp_count,
        null as no_of_visits,
        'EDETAILING' as datasource

    from detailing_va_hcp_jn

    union all

    select

        null as va_name,
        null as page_name,
        team_name,
        null as month,
        rbm,
        zbm,
        fbm,
        null as msr,
        hq,
        null as msr_name,
        null as dsg,
        null as customer_name,
        null as cid,
        null as speciality,
        null as customer_type,
        null as active,
        null as dcr_date,
        null as page_start_time,
        null as page_end_time,
        null as seconds,
        null as capped_seconds,
        null as as_on,
        null as franchise,
        null as sub_group,
        null as group_name,
        'NA' as brand,
        null as v_custid,
        cust_spec,
        core_noncore,
        calls_planned,
        'Y' as is_active,
        dcr_month,
        dcr_year,
        v_terrid,
        distinct_hcp_count,
        null as no_of_visits,
        'HCP_MASTER' as datasource

    from
        region_filter_hcp

    union all
    select
        null as va_name,
        null as page_name,
        team_name,
        null as month,
        rbm,
        zbm,
        fbm,
        null as msr,
        hq,
        null as msr_name,
        null as dsg,
        null as customer_name,
        null as cid,
        null as speciality,
        null as customer_type,
        null as active,
        null as dcr_date,
        null as page_start_time,
        null as page_end_time,
        null as seconds,
        null as capped_seconds,
        null as as_on,
        null as franchise,
        null as sub_group,
        null as group_name,
        'NA' as brand,
        v_custid,
        cust_spec,
        core_noncore,
        calls_planned,
        'Y' as is_active,
        dcr_month,
        dcr_year,
        v_terrid,
        distinct_hcp_count,
        no_of_visits,
        'DCR_DATA' as datasource

    from dcr_hcp_region_join
)

select  * from final_edetailing_datamart