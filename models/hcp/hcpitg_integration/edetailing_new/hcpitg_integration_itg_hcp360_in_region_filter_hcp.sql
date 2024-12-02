with ventasys_hcp_master AS (
    SELECT *
    FROM {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcp_master') }}
),

territory_master as(
    select *
    from DEV_DNA_CORE.HCPITG_INTEGRATION.ITG_HCP360_IN_VENTASYS_TERRITORY_MASTER
    
),






region_filter_hcp as (

Select hcp.team_name,
hcp.v_terrid,
vtm.lvl1 as RBM,
vtm.lvl2 as ZBM,
vtm.lvl3 as FBM,
vtm.hq,
hcp.cust_spec,
hcp.core_noncore,
count(distinct(hcp.v_custid)) as distinct_hcp_count,
'Hcp_master' as datasource,
'4' as dcr_month,
'2024' as dcr_year

from ventasys_hcp_master hcp
left join territory_master vtm
on hcp.v_terrid= vtm.v_terrid 
where hcp.is_active='Y'
group by 1,2,3,4,5,6,7,8

union all 

Select hcp.team_name,
hcp.v_terrid,
vtm.lvl1 as RBM,
vtm.lvl2 as ZBM,
vtm.lvl3 as FBM,
vtm.hq,
hcp.cust_spec,
hcp.core_noncore,
count(distinct(hcp.v_custid)) as distinct_hcp_count,
'Hcp_master' as datasource,
'5' as dcr_month,
'2024' as dcr_year

from ventasys_hcp_master hcp
left join territory_master vtm
on hcp.v_terrid= vtm.v_terrid 
where hcp.is_active='Y'
group by 1,2,3,4,5,6,7,8

)
select * from region_filter_hcp 