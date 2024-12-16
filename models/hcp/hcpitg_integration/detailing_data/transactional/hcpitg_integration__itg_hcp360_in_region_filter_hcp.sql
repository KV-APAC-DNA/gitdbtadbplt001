with ventasys_hcp_master AS (
    SELECT *
    FROM {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcp_master') }}
),

territory_master as(
    select *
    from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_territory_master') }}

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
CASE
        WHEN hcp.core_noncore = 'Core' THEN 2
        WHEN hcp.core_noncore = 'Non Core' THEN 1
    ELSE NULL
END AS calls_planned,
'04' as dcr_month,
'2024' as dcr_year,
count(distinct(hcp.v_custid)) as distinct_hcp_count

from ventasys_hcp_master hcp
left join territory_master vtm
on hcp.v_terrid= vtm.v_terrid 
where hcp.is_active='Y'
group by 1,2,3,4,5,6,7,8,9,10,11

union all 

Select hcp.team_name,
hcp.v_terrid,
vtm.lvl1 as RBM,
vtm.lvl2 as ZBM,
vtm.lvl3 as FBM,
vtm.hq,
hcp.cust_spec,
hcp.core_noncore,
CASE
        WHEN hcp.core_noncore = 'Core' THEN 2
        WHEN hcp.core_noncore = 'Non Core' THEN 1
    ELSE NULL
END AS calls_planned,
'05' as dcr_month,
'2024' as dcr_year,
count(distinct(hcp.v_custid)) as distinct_hcp_count

from ventasys_hcp_master hcp
left join territory_master vtm
on hcp.v_terrid= vtm.v_terrid 
where hcp.is_active='Y'
group by 1,2,3,4,5,6,7,8,9,10,11

)
select * from region_filter_hcp