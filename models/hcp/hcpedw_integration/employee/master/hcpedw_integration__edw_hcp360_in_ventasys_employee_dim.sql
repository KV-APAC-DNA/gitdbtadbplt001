with itg_hcp360_in_ventasys_employee_master as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_employee_master') }}
),
itg_hcp360_in_ventasys_territory_master as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_territory_master') }}
),
final as
(
    SELECT EMP.V_EMPID AS EMPLOYEE_ID,
           EMP.V_TERRID AS EMP_TERRID,
           EMP.V_MGR_EMPID AS MGR_EMPLOYEE_ID,
           TERR.LVL1 AS Region,
           TERR.LVL2 AS Zone,
           TERR.LVL3 AS Territory,
           TERR.HQ AS EMP_HQ_NAME,
           EMP.TEAM_NAME,
           EMP.TEAM_NAME AS TEAM_BRAND_NAME,
           EMP.NAME,
           EMP.DSG AS DESIGNATION,
           EMP.PHONE,
           EMP.EMAIL,
           EMP.CITY,
           EMP.STATE,
           EMP.JOIN_DT AS JOIN_DATE,
           EMP.IS_ACTIVE,
           EMP.REL_DT AS REL_DATE,
           MGR.Level_2_EMP,
           MGR.Level_2_EMP_Name,
           MGR.Level_1_EMP,
           MGR.Level_1_EMP_Name,
		   convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM,
		   convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
        FROM ITG_HCP360_IN_VENTASYS_EMPLOYEE_MASTER EMP
             ,(SELECT EMP1.V_EMPID Level_2_EMP, EMP1.NAME Level_2_EMP_Name ,EMP1.V_MGR_EMPID Level_2_Mgr, 
                      EMP2.V_EMPID Level_1_EMP, EMP2.NAME Level_1_EMP_Name, EMP2.V_MGR_EMPID  Level_1_Mgr
                   FROM ITG_HCP360_IN_VENTASYS_EMPLOYEE_MASTER as  EMP1                    
                       ,ITG_HCP360_IN_VENTASYS_EMPLOYEE_MASTER as EMP2 
                 WHERE EMP1.V_MGR_EMPID = EMP2.V_EMPID (+) 
                 ) MGR,
                 ITG_HCP360_IN_VENTASYS_TERRITORY_MASTER TERR
        WHERE EMP.V_TERRID = TERR.V_TERRID
         AND EMP.TEAM_NAME = TERR.TEAM_NAME
         AND EMP.V_MGR_EMPID = MGR.Level_2_EMP (+)
)
select employee_id::varchar(20) as employee_id,
    emp_terrid::varchar(20) as emp_terrid,
    mgr_employee_id::varchar(20) as mgr_employee_id,
    region::varchar(50) as region,
    zone::varchar(50) as zone,
    territory::varchar(50) as territory,
    emp_hq_name::varchar(100) as emp_hq_name,
    team_name::varchar(20) as team_name,
    team_brand_name::varchar(20) as team_brand_name,
    name::varchar(100) as name,
    designation::varchar(20) as designation,
    phone::varchar(50) as phone,
    email::varchar(100) as email,
    city::varchar(50) as city,
    state::varchar(50) as state,
    join_date::date as join_date,
    is_active::varchar(10) as is_active,
    rel_date::date as rel_date,
    level_2_emp::varchar(20) as level_2_emp,
    level_2_emp_name::varchar(50) as level_2_emp_name,
    level_1_emp::varchar(20) as level_1_emp,
    level_1_emp_name::varchar(50) as level_1_emp_name,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final