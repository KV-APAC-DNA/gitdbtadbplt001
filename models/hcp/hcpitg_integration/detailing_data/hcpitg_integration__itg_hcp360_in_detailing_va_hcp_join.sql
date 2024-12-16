with detailing_data as (
    select *
    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_edetailing_raw') }}
),

sdl_va_page_class as (
    select *
    from
        {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_va_page_raw') }}
),

ventasys_hcp_master as (
    select *
    from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcp_master') }}
),


detailing_va_hcp_join as (
    select
        edr.va_name,
        edr.page_name,
        CASE WHEN edr.TEAM = 'EM' THEN 'DERMA' ELSE edr.TEAM END AS team,
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
        CASE
            WHEN hcp.core_noncore = 'Core' THEN 2
            WHEN hcp.core_noncore = 'Non Core' THEN 1
            ELSE NULL
        END AS calls_planned,
        hcp.is_active,
        hcp.v_terrid,
        to_char(to_date(edr.dcr_date), 'MM') as dcr_month,
        to_char(to_date(edr.dcr_date), '20'||'YY') as dcr_year

    from detailing_data as edr
    left join (select * from (
            select vapc.FRANCHISE, 
                vapc.SUB_GROUP, 
                vapc.GROUP_NAME, 
                    vapc.BRAND, 
                    vapc.va_name,
                    vapc.page_name ,row_number() over (partition by vapc.va_name,vapc.page_name order by va_name desc) as rnk from 
      sdl_va_page_class vapc) where rnk=1) vapc
        on edr.va_name = vapc.va_name and edr.page_name = vapc.page_name
    left join ventasys_hcp_master as hcp
        on concat('C', edr.cid) = hcp.v_custid
    where edr.CUSTOMER_TYPE ='Doctor'
)

select *  from detailing_va_hcp_join