with
itg_hcp360_in_ventasys_rtlmaster as (
    select *
    from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_rtlmaster') }}
),

itg_hcp360_in_ventasys_hcprtl as (
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcprtl') }}
),

itg_hcp360_in_ventasys_rxrtl as (
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_rxrtl') }}
),

edw_hcp360_in_ventasys_hcp_dim_latest as (
    select *
    from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_hcp_dim_latest') }}
),

edw_hcp360_in_ventasys_employee_dim as (
    select *
    from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_employee_dim') }}
),

edw_rpt_pob_cx_final as (
    select * from {{ ref('hcpedw_integration__edw_rpt_pob_cx_final') }}
),

edw_product_dim as (
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),

edw_retailer_calendar_dim as (
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),

itg_ventasys_jnj_prod_mapping as (
    select *
    from {{ source('inditg_integration', 'itg_ventasys_jnj_prod_mapping') }}
),

edw_rpt_sales_details as (
    select * from {{ ref('indedw_integration__edw_rpt_sales_details') }}
),

edw_retailer_calendar_dim as (
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),

edw_retailer_dim as (
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),

edw_customer_dim as (
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),

itg_in_rcustomerroute as (
    select * from {{ ref('inditg_integration__itg_in_rcustomerroute') }}
),

itg_in_rroute as (
    select * from {{ ref('inditg_integration__itg_in_rroute') }}
),

itg_in_rsalesmanroute as (
    select * from {{ ref('inditg_integration__itg_in_rsalesmanroute') }}
),

itg_in_rsalesman as (
    select * from {{ ref('inditg_integration__itg_in_rsalesman') }}
),

wks_rx_to_cx_to_pob_base_rtl as (
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_base_rtl') }}
),

wks_rx_to_cx_to_pob_salesman_master as (
    select *
    from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_salesman_master') }}
),

wks_rx_to_cx_to_pob_sales_cube as (
    select *
    from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_sales_cube') }}
),

wks_rx_to_cx_to_pob_rtl_dim as (
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_rtl_dim') }}
),

wks_rx_to_cx_to_pob_rxrtl as (
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob_rxrtl') }}
),

itg_query_parameters as (
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),

wks_rx_to_cx_to_pob_rxrtl_urc as (
    select
        rtl.urc,
        rxrtl.rx_product,
        SUM(rxrtl.rx_units) as rx_units,
        rxrtl.quarter,
        rxrtl.month,
        rxrtl.year
    from wks_rx_to_cx_to_pob_rxrtl as rxrtl
    left join (select
        urc,
        v_custid_rtl
    --ROW_NUMBER() OVER (PARTITION BY urc ORDER BY v_custid_rtl DESC) AS rnk
    from itg_hcp360_in_ventasys_rtlmaster
    where
        urc is not NULL
        and is_active = 'Y'
    group by 1, 2) as rtl
        on rxrtl.v_custid_rtl = rtl.v_custid_rtl
    group by 1, 2, 4, 5, 6
),

final as (
    select
        cal."year",
        cal."month",
        rtl.urc,
        rtl.is_active as "URC Active Flag Ventasys",
        mapp.prod_vent as ventasys_product,
        prod.franchise_name,
        prod.brand_name,
        sales.quantity as sales_qty,
        sales.achievement_nr as sales_ach_nr,
        pob.pob_units,
        hcprtl.v_custid_dr as hcp_id,
        rxrtl.rx_units as rx_factorized,
        rxrtl.rx_units * 4 as rx_units,
        COALESCE(rd.retailer_name, sd.retailer_name) as urc_name,
        COALESCE(rd.customer_code, sd.latest_customer_code) as distributor_code,
        COALESCE(rd.customer_name, sd.customer_name) as distributor_name,
        COALESCE(rd.region_name, sd.region_name) as region_name,
        COALESCE(rd.zone_name, sd.zone_name) as zone_name,
        COALESCE(rd.territory_name, sd.territory_name) as territory_name,
        COALESCE(rd.channel_name, sd.channel_name) as channel_name,
        COALESCE(rd.class_desc, sd.class_desc) as class_desc,
        COALESCE(rd.retailer_category_name, sd.retailer_category_name)
            as retailer_category_name,
        COALESCE(rd.retailer_channel_level1, sd.retailer_channel_1)
            as retailer_channel_1,
        COALESCE(rd.retailer_channel_level2, sd.retailer_channel_2)
            as retailer_channel_2,
        COALESCE(rd.retailer_channel_level3, sd.retailer_channel_3)
            as retailer_channel_3,
        COALESCE(rd.urc_active_flag, 'N') as "URC Active Flag",
        sm.smcode as salesman_code_sales,
        sm.smname as salesman_name_sales,
        hcpdim.customer_name as hcp_name,
        hcpdim.name as emp_name,
        hcpdim.employee_id as emp_id,
        hcpdim.region as region_vent,
        hcpdim.territory as territory_vent,
        hcpdim.zone as zone_vent
    from (
        select *
        from wks_rx_to_cx_to_pob_base_rtl
    ) as rtl
    cross join (
        select
            LEFT(mth_mm, 4) as "year",
            RIGHT(mth_mm, 2) as "month"
        from edw_retailer_calendar_dim
        where
            fisc_yr >= EXTRACT(year from CURRENT_TIMESTAMP()) - 2
            and day <= TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD')
        group by 1, 2
    ) as cal
    cross join (
        select prod_vent
        from (
            select SPLIT_PART(parameter_name, '-', 1) as prod_vent
            from itg_query_parameters
            where parameter_type = 'Rx_to_Cx_to_Pob_Product_Mapping'
            group by 1
        )
        --WHERE UPPER(prod_vent) LIKE 'ORSL%'
        group by 1
    ) as mapp
    left join (
        select
            franchise_name,
            brand_name,
            pmap.prod_vent
        from edw_product_dim as pd
        inner join (select
            SPLIT_PART(parameter_name, '-', 1) as prod_vent,
            SPLIT_PART(parameter_value, '-', 1) as product_code
        from itg_query_parameters
        where parameter_type = 'Rx_to_Cx_to_Pob_Product_Mapping'
        group by 1, 2) as pmap
            on pd.product_code = pmap.product_code
        group by 1, 2, 3
    ) as prod
        on UPPER(mapp.prod_vent) = UPPER(prod.prod_vent)
    left join (
        select
            LEFT(sd.mth_mm, 4) as year,
            RIGHT(sd.mth_mm, 2) as month,
            sd.rtruniquecode,
            pmap.prod_vent,
            SUM(quantity) as quantity,
            SUM(achievement_nr) as achievement_nr
        from edw_rpt_sales_details as sd
        inner join (select
            SPLIT_PART(parameter_name, '-', 1) as prod_vent,
            SPLIT_PART(parameter_value, '-', 1) as product_code
        from itg_query_parameters
        where parameter_type = 'Rx_to_Cx_to_Pob_Product_Mapping'
        group by 1, 2) as pmap
            on pmap.product_code = sd.product_code
            --AND UPPER(prod_vent) LIKE 'ORSL%'
        where sd.fisc_yr >= EXTRACT(year from CURRENT_TIMESTAMP()) - 2
        group by 1, 2, 3, 4
    ) as sales
        on
            cal."year" = sales.year
            and cal."month" = sales.month
            and rtl.urc::text = sales.rtruniquecode
            and mapp.prod_vent = sales.prod_vent
    left join (
        select
            year,
            month,
            urc,
            ventasys_product,
            SUM(sales_qty) as sales_qty,
            SUM(sales_ach_nr) as sales_ach_nr,
            SUM(pob_units) as pob_units
        from edw_rpt_pob_cx_final
        where year >= EXTRACT(year from CURRENT_TIMESTAMP()) - 2
        --AND UPPER(ventasys_product) LIKE 'ORSL%'
        group by
            1,
            2,
            3,
            4
    ) as pob
        on
            cal."year" = pob.year
            and LTRIM(cal."month", 0) = pob.month
            and rtl.urc = pob.urc
            and mapp.prod_vent = pob.ventasys_product
    left join (
        select
            v_custid_rtl,
            v_custid_dr,
            ROW_NUMBER()
                over (partition by v_custid_rtl order by filename desc)
                as rnk
        from itg_hcp360_in_ventasys_hcprtl
    ) as hcprtl
        on
            rtl.v_custid_rtl = hcprtl.v_custid_rtl
            and hcprtl.rnk = 1
    left join (select * from wks_rx_to_cx_to_pob_rxrtl_urc) as rxrtl
        on
            cal."year" = rxrtl.year
            and LTRIM(cal."month", 0) = LTRIM(rxrtl.month, 0)
            and rtl.urc = rxrtl.urc
            and mapp.prod_vent = rxrtl.rx_product
    left join (
        select *
        from wks_rx_to_cx_to_pob_rtl_dim
    ) as rd
        on rd.rtruniquecode = rtl.urc::text
    left join (
        select *
        from wks_rx_to_cx_to_pob_sales_cube
    ) as sd
        on rtl.urc::text = sd.rtruniquecode
    left join (
        select *
        from wks_rx_to_cx_to_pob_salesman_master
    ) as sm
        on
            COALESCE(rd.customer_code, sd.latest_customer_code) = sm.distcode
            and rd.retailer_code = sm.rtrcode
    left join (
        select
            hcp.hcp_id,
            hcp.customer_name,
            emp.name,
            emp.employee_id,
            emp.region,
            emp.territory,
            emp.zone
        from edw_hcp360_in_ventasys_hcp_dim_latest as hcp
        left join (
            select
                emp_terrid,
                name,
                employee_id,
                region,
                territory,
                zone,
                ROW_NUMBER()
                    over (partition by emp_terrid order by join_date desc)
                    as rnk
            from edw_hcp360_in_ventasys_employee_dim
            where is_active = 'Y'
        ) as emp
            on
                hcp.territory_id = emp.emp_terrid
                and emp.rnk = 1
        where hcp.valid_to > CURRENT_DATE
    ) as hcpdim
        on hcprtl.v_custid_dr = hcpdim.hcp_id
)

select * from final
