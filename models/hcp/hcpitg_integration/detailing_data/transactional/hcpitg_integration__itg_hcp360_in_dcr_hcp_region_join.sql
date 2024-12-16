WITH dcr_data as (
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_dcrdata') }}
),

ventasys_hcp_master as (
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcp_master') }}
),

territory_master as(
    select *
    from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_territory_master') }}

),

dcr_hcp_region_join as (
select dcr_year,
dcr_month,
team_name,
v_terrid,
RBM,
ZBM,
FBM,
hq,
cust_spec,
core_noncore,
CASE
        WHEN core_noncore = 'Core' THEN 2
        WHEN core_noncore = 'Non Core' THEN 1
    ELSE NULL
END AS calls_planned,
v_custid,
count(distinct(v_custid)) as distinct_hcp_count,
count(distinct(dcr_dt)) as no_of_visits,

from (

    select hcp.team_name,
    hcp.v_terrid,
    vtm.lvl1 as RBM,
    vtm.lvl2 as ZBM,
    vtm.lvl3 as FBM,
    vtm.hq,
    hcp.cust_spec,
    hcp.v_custid,
    dcr.dcr_dt,
    to_char(to_date(dcr.dcr_dt),'MM') as dcr_month,
    to_char(to_date(dcr.dcr_dt),'20'||'YY') as dcr_year,
    hcp.core_noncore

from  dcr_data dcr
left join  ventasys_hcp_master hcp
on hcp.v_custid=dcr.v_custid
left join territory_master vtm
on vtm.v_terrid=hcp.v_terrid
where is_active='Y')
group by 1,2,3,4,5,6,7,8,9,10,11,12

)
select * from dcr_hcp_region_join