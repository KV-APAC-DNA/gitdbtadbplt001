with detailing_data as (
    select *
    from dev_dna_core.hcpitg_integration.sdl_hcp360_in_ventasys_edetailing_raw
),

sdl_va_page_class as (
    select *
    from
        dev_dna_core.hcpitg_integration.sdl_hcp360_in_ventasys_va_page_raw
),

ventasys_hcp_master as (
    select *
    from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcp_master') }}
),


detailing_va_hcp_join as (
    select
        edr.va_name,
        edr.page_name,
        edr.team,
        edr.month,
        edr.rbm,
        edr.zbm,
        edr.fbm,
        edr.msr,
        edr.hq_code,
        edr.msr_name,
        edr.dsg,
        edr.customer_name,
        edr.cid,
        edr.speciality,
        edr.customer_type,
        edr.active,
        edr.dcr_date,
        edr.page_start_time,
        edr.page_end_time,
        edr.seconds,
        edr.capped_seconds,
        edr.as_on,
        vapc.franchise,
        vapc.sub_group,
        vapc.group_name,
        vapc.brand,
        hcp.v_custid,
        hcp.cust_spec,
        hcp.core_noncore,
        hcp.is_active,
        hcp.v_terrid,
        'Edetailing' as datasource,
        to_char(to_date(edr.dcr_date), 'MM') as dcr_month,
        to_char(to_date(edr.dcr_date), '20' || 'YY') as dcr_year

    from detailing_data as edr
    left join sdl_va_page_class as vapc
        on edr.va_name = vapc.va_name and edr.page_name = vapc.page_name
    left join ventasys_hcp_master as hcp
        on concat('C', edr.cid) = hcp.v_custid
)

select *  from detailing_va_hcp_join