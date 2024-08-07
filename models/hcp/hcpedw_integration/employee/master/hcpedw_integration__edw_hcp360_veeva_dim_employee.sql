with edw_hcp360_veeva_wrk_dim_employee as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_wrk_dim_employee') }}
),
final as
(
    select
    IQ.EMPLOYEE_KEY,
    IQ.COUNTRY_CODE,
    NVL(IQ.EMPLOYEE_SOURCE_ID,'Not Applicable') as EMPLOYEE_SOURCE_ID,
    IQ.LAST_MODIFIED_DATE::timestamp_ntz as MODIFIED_DT,
    NVL(IQ.LAST_MODIFIED_BY_ID,'Not Applicable') as MODIFIED_ID,
    IQ.EMPLOYEE_NAME,
    IQ.WWID AS EMPLOYEE_WWID,
    IQ.MOBILE_PHONE,
    IQ.EMAIL AS EMAIL_ID,
    IQ.USER_NAME as USERNAME,
    IQ.NICKNAME,
    IQ.LOCAL_EMPLOYEE_NUMBER,
    null as PROFILE_ID,
    IQ.COMPANY_NAME,
    IQ.DIVISION as DIVISION_NAME,
    IQ.DEPARTMENT as DEPARTMENT_NAME,
    IQ.COUNTRY as COUNTRY_NAME,
    IQ.ADDRESS,
    IQ.ALIAS,
    IQ.TIMEZONESIDKEY,
    IQ.RECEIVES_INFO_EMAILS,
    IQ.FEDERATION_IDENTIFIER,
    IQ.LAST_IPAD_SYNC::timestamp_ntz as LAST_IPAD_SYNC,
    IQ.USER_LICENSE,    
    IQ.TITLE, 
    IQ.PHONE, 
    IQ.LAST_LOGIN_DATE::timestamp_ntz as LAST_LOGIN_DATE, 
    IQ.REGION, 
    IQ.MANAGER_NAME, 
    IQ.MANAGER_WWID, 
    IQ.ACTIVE_FLAG,
    IQ.MY_ORGANIZATION_CODE,
    IQ.MY_ORGANIZATION_NAME,
    nvl(IQ.ORGANIZATION_L1_CODE,'Not Available') as ORGANIZATION_L1_CODE,
    nvl(IQ.ORGANIZATION_L1_NAME,'Not Available') as ORGANIZATION_L1_NAME ,
    nvl(case when IQ.ORGANIZATION_L2_CODE = IQ.ORGANIZATION_L1_CODE then 'Not Available' else IQ.ORGANIZATION_L2_CODE end ,'Not Available') as ORGANIZATION_L2_CODE,
    nvl(case when IQ.ORGANIZATION_L2_NAME = IQ.ORGANIZATION_L1_NAME then 'Not Available' else IQ.ORGANIZATION_L2_NAME end ,'Not Available') as ORGANIZATION_L2_NAME,
    nvl(case when IQ.ORGANIZATION_L3_CODE = IQ.ORGANIZATION_L2_CODE then 'Not Available' else IQ.ORGANIZATION_L3_CODE end ,'Not Available') as ORGANIZATION_L3_CODE,
    nvl(case when IQ.ORGANIZATION_L3_NAME = IQ.ORGANIZATION_L2_NAME then 'Not Available' else IQ.ORGANIZATION_L3_NAME end ,'Not Available') as ORGANIZATION_L3_NAME,
    nvl(case when IQ.ORGANIZATION_L4_CODE = IQ.ORGANIZATION_L3_CODE then 'Not Available' else IQ.ORGANIZATION_L4_CODE end ,'Not Available') as ORGANIZATION_L4_CODE,
    nvl(case when IQ.ORGANIZATION_L4_NAME = IQ.ORGANIZATION_L3_NAME then 'Not Available' else IQ.ORGANIZATION_L4_NAME end ,'Not Available') as ORGANIZATION_L4_NAME,
    nvl(case when IQ.ORGANIZATION_L5_CODE = IQ.ORGANIZATION_L4_CODE then 'Not Available' else IQ.ORGANIZATION_L5_CODE end ,'Not Available') as ORGANIZATION_L5_CODE,
    nvl(case when IQ.ORGANIZATION_L5_NAME = IQ.ORGANIZATION_L4_NAME then 'Not Available' else IQ.ORGANIZATION_L5_NAME end ,'Not Available') as ORGANIZATION_L5_NAME,
    nvl(case when IQ.ORGANIZATION_L6_CODE = IQ.ORGANIZATION_L5_CODE then 'Not Available' else IQ.ORGANIZATION_L6_CODE end ,'Not Available') as ORGANIZATION_L6_CODE,
    nvl(case when IQ.ORGANIZATION_L6_NAME = IQ.ORGANIZATION_L5_NAME then 'Not Available' else IQ.ORGANIZATION_L6_NAME end ,'Not Available') as ORGANIZATION_L6_NAME,
    nvl(case when IQ.ORGANIZATION_L7_CODE = IQ.ORGANIZATION_L6_CODE then 'Not Available' else IQ.ORGANIZATION_L7_CODE end ,'Not Available') as ORGANIZATION_L7_CODE,
    nvl(case when IQ.ORGANIZATION_L7_NAME = IQ.ORGANIZATION_L6_NAME then 'Not Available' else IQ.ORGANIZATION_L7_NAME end ,'Not Available') as ORGANIZATION_L7_NAME,
    nvl(case when IQ.ORGANIZATION_L8_CODE = IQ.ORGANIZATION_L7_CODE then 'Not Available' else IQ.ORGANIZATION_L8_CODE end ,'Not Available') as ORGANIZATION_L8_CODE,
    nvl(case when IQ.ORGANIZATION_L8_NAME = IQ.ORGANIZATION_L7_NAME then 'Not Available' else IQ.ORGANIZATION_L8_NAME end ,'Not Available') as ORGANIZATION_L8_NAME,
    nvl(case when IQ.ORGANIZATION_L9_CODE = IQ.ORGANIZATION_L8_CODE then 'Not Available' else IQ.ORGANIZATION_L9_CODE end ,'Not Available') as ORGANIZATION_L9_CODE,
    nvl(case when IQ.ORGANIZATION_L9_NAME = IQ.ORGANIZATION_L8_NAME then 'Not Available' else IQ.ORGANIZATION_L9_NAME end ,'Not Available') as ORGANIZATION_L9_NAME,
    nvl(IQ.COMMON_ORGANIZATION_L1_CODE,'Not Available') as COMMON_ORGANIZATION_L1_CODE,
    nvl(IQ.COMMON_ORGANIZATION_L1_NAME,'Not Available') as COMMON_ORGANIZATION_L1_NAME,
    nvl(case when IQ.COMMON_ORGANIZATION_L2_CODE = IQ.COMMON_ORGANIZATION_L1_CODE then 'Not Available' else IQ.COMMON_ORGANIZATION_L2_CODE end ,'Not Available') as COMMON_ORGANIZATION_L2_CODE,
    nvl(case when IQ.COMMON_ORGANIZATION_L2_NAME = IQ.COMMON_ORGANIZATION_L1_NAME then 'Not Available' else IQ.COMMON_ORGANIZATION_L2_NAME end ,'Not Available') as COMMON_ORGANIZATION_L2_NAME,
    nvl(case when IQ.COMMON_ORGANIZATION_L3_CODE = IQ.COMMON_ORGANIZATION_L2_CODE then 'Not Available' else IQ.COMMON_ORGANIZATION_L3_CODE end ,'Not Available') as COMMON_ORGANIZATION_L3_CODE,
    nvl(case when IQ.COMMON_ORGANIZATION_L3_NAME = IQ.COMMON_ORGANIZATION_L2_NAME then 'Not Available' else IQ.COMMON_ORGANIZATION_L3_NAME end ,'Not Available') as COMMON_ORGANIZATION_L3_NAME,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm,
    IQ.MSL_PRIMARY_RESPONSIBLE_TA,
    null as MSL_SECONDARY_RESPONSIBLE_TA
    from 
    (SELECT 
    EMPLOYEE_KEY,
    COUNTRY_CODE,
    EMPLOYEE_SOURCE_ID,
    LAST_MODIFIED_DATE,
    LAST_MODIFIED_BY_ID,
    EMPLOYEE_NAME,
    WWID,
    MOBILE_PHONE,
    EMAIL,
    USER_NAME,
    NICKNAME,
    LOCAL_EMPLOYEE_NUMBER,
    COMPANY_NAME,
    DIVISION,
    DEPARTMENT,
    COUNTRY,
    ADDRESS,
    ALIAS,
    TIMEZONESIDKEY,
    RECEIVES_INFO_EMAILS,
    FEDERATION_IDENTIFIER,
    LAST_IPAD_SYNC,
    USER_LICENSE,    
    TITLE, 
    PHONE, 
    LAST_LOGIN_DATE, 
    REGION, 
    MANAGER_NAME, 
    MANAGER_WWID, 
    ACTIVE_FLAG,
    CASE WHEN MY_ORGANIZATION_CODE ='' THEN 'Not Available' ELSE MY_ORGANIZATION_CODE END AS MY_ORGANIZATION_CODE,
    CASE WHEN MY_ORGANIZATION_NAME ='' THEN 'Not Available' ELSE MY_ORGANIZATION_NAME END AS MY_ORGANIZATION_NAME,
    CASE WHEN ORGANIZATION_L1_CODE ='' THEN 'Not Available' ELSE ORGANIZATION_L1_CODE END AS ORGANIZATION_L1_CODE,
    CASE WHEN ORGANIZATION_L1_NAME ='' THEN 'Not Available' ELSE ORGANIZATION_L1_NAME END AS ORGANIZATION_L1_NAME ,
    CASE WHEN ORGANIZATION_L2_CODE ='' THEN 'Not Available' ELSE ORGANIZATION_L2_CODE END AS ORGANIZATION_L2_CODE,
    CASE WHEN ORGANIZATION_L2_NAME ='' THEN 'Not Available' ELSE ORGANIZATION_L2_NAME END AS ORGANIZATION_L2_NAME ,
    CASE WHEN ORGANIZATION_L3_CODE ='' THEN 'Not Available' ELSE ORGANIZATION_L3_CODE END AS ORGANIZATION_L3_CODE,
    CASE WHEN ORGANIZATION_L3_NAME ='' THEN 'Not Available' ELSE ORGANIZATION_L3_NAME END AS ORGANIZATION_L3_NAME ,
    CASE WHEN ORGANIZATION_L4_CODE ='' THEN 'Not Available' ELSE ORGANIZATION_L4_CODE END AS ORGANIZATION_L4_CODE,
    CASE WHEN ORGANIZATION_L4_NAME ='' THEN 'Not Available' ELSE ORGANIZATION_L4_NAME END AS ORGANIZATION_L4_NAME ,
    CASE WHEN ORGANIZATION_L5_CODE ='' THEN 'Not Available' ELSE ORGANIZATION_L5_CODE END AS ORGANIZATION_L5_CODE,
    CASE WHEN ORGANIZATION_L5_NAME ='' THEN 'Not Available' ELSE ORGANIZATION_L5_NAME END AS ORGANIZATION_L5_NAME ,
    CASE WHEN ORGANIZATION_L6_CODE ='' THEN 'Not Available' ELSE ORGANIZATION_L6_CODE END AS ORGANIZATION_L6_CODE,
    CASE WHEN ORGANIZATION_L6_NAME ='' THEN 'Not Available' ELSE ORGANIZATION_L6_NAME END AS ORGANIZATION_L6_NAME ,
    CASE WHEN ORGANIZATION_L7_CODE ='' THEN 'Not Available' ELSE ORGANIZATION_L7_CODE END AS ORGANIZATION_L7_CODE,
    CASE WHEN ORGANIZATION_L7_NAME ='' THEN 'Not Available' ELSE ORGANIZATION_L7_NAME END AS ORGANIZATION_L7_NAME ,
    CASE WHEN ORGANIZATION_L8_CODE ='' THEN 'Not Available' ELSE ORGANIZATION_L8_CODE END AS ORGANIZATION_L8_CODE,
    CASE WHEN ORGANIZATION_L8_NAME ='' THEN 'Not Available' ELSE ORGANIZATION_L8_NAME END AS ORGANIZATION_L8_NAME ,
    CASE WHEN ORGANIZATION_L9_CODE ='' THEN 'Not Available' ELSE ORGANIZATION_L9_CODE END AS ORGANIZATION_L9_CODE,
    CASE WHEN ORGANIZATION_L9_NAME ='' THEN 'Not Available' ELSE ORGANIZATION_L9_NAME END AS ORGANIZATION_L9_NAME ,
    CASE WHEN COMMON_ORGANIZATION_L1_CODE ='' THEN 'Not Available' ELSE COMMON_ORGANIZATION_L1_CODE END  AS COMMON_ORGANIZATION_L1_CODE,
    CASE WHEN COMMON_ORGANIZATION_L1_NAME ='' THEN 'Not Available' ELSE COMMON_ORGANIZATION_L1_NAME END  AS COMMON_ORGANIZATION_L1_NAME,
    CASE WHEN COMMON_ORGANIZATION_L2_CODE ='' THEN 'Not Available' ELSE COMMON_ORGANIZATION_L2_CODE END AS COMMON_ORGANIZATION_L2_CODE,
    CASE WHEN COMMON_ORGANIZATION_L2_NAME ='' THEN 'Not Available' ELSE COMMON_ORGANIZATION_L2_NAME END AS COMMON_ORGANIZATION_L2_NAME ,
    CASE WHEN COMMON_ORGANIZATION_L3_CODE ='' THEN 'Not Available' ELSE COMMON_ORGANIZATION_L3_CODE END AS COMMON_ORGANIZATION_L3_CODE,
    CASE WHEN COMMON_ORGANIZATION_L3_NAME ='' THEN 'Not Available' ELSE COMMON_ORGANIZATION_L3_NAME END AS COMMON_ORGANIZATION_L3_NAME ,
    MSL_PRIMARY_RESPONSIBLE_TA
    FROM (SELECT EQ.*,ROW_NUMBER() OVER (PARTITION BY EQ.EMPLOYEE_SOURCE_ID ORDER BY EQ.EMPLOYEE_SOURCE_ID,EQ.RNK) AS ROW_NUM FROM edw_hcp360_veeva_wrk_dim_employee EQ)  
    WHERE ROW_NUM = 1 
    ) IQ
    
    UNION ALL
    
    SELECT 'Not Applicable','ZZ','Not Applicable',convert_timezone('UTC',current_timestamp())::timestamp_ntz,'Not Applicable','Not Applicable','Not Applicable',
    'Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable',
    'Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable',
    0,'Not Applicable',convert_timezone('UTC',current_timestamp())::timestamp_ntz,'Not Applicable',
    'Not Applicable','Not Applicable',convert_timezone('UTC',current_timestamp())::timestamp_ntz,'Not Applicable',
    'Not Applicable','Not Applicable',0,'Not Applicable','Not Applicable','Not Applicable','Not Applicable',
    'Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable',
    'Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable',
    'Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable',
    'Not Applicable','Not Applicable','Not Applicable','Not Applicable',
    convert_timezone('UTC',current_timestamp())::timestamp_ntz,convert_timezone('UTC',current_timestamp())::timestamp_ntz,'Not Applicable','Not Applicable'
)
select employee_key::varchar(32) as employee_key,
    country_code::varchar(8) as country_code,
    employee_source_id::varchar(18) as employee_source_id,
    modified_dt::timestamp_ntz(9) as modified_dt,
    modified_id::varchar(18) as modified_id,
    employee_name::varchar(121) as employee_name,
    employee_wwid::varchar(20) as employee_wwid,
    mobile_phone::varchar(40) as mobile_phone,
    email_id::varchar(128) as email_id,
    username::varchar(80) as username,
    nickname::varchar(40) as nickname,
    local_employee_number::varchar(20) as local_employee_number,
    profile_id::varchar(18) as profile_id,
    company_name::varchar(80) as company_name,
    division_name::varchar(80) as division_name,
    department_name::varchar(80) as department_name,
    country_name::varchar(80) as country_name,
    address::varchar(255) as address,
    alias::varchar(18) as alias,
    timezonesidkey::varchar(40) as timezonesidkey,
    receives_info_emails::number(18,0) as receives_info_emails,
    federation_identifier::varchar(512) as federation_identifier,
    last_ipad_sync::timestamp_ntz(9) as last_ipad_sync,
    user_license::varchar(1300) as user_license,
    title::varchar(1300) as title,
    phone::varchar(43) as phone,
    last_login_date::timestamp_ntz(9) as last_login_date,
    region::varchar(1300) as region,
    manager_name::varchar(80) as manager_name,
    manager_wwid::varchar(18) as manager_wwid,
    active_flag::number(2,0) as active_flag,
    my_organization_code::varchar(18) as my_organization_code,
    my_organization_name::varchar(80) as my_organization_name,
    organization_l1_code::varchar(18) as organization_l1_code,
    organization_l1_name::varchar(80) as organization_l1_name,
    organization_l2_code::varchar(18) as organization_l2_code,
    organization_l2_name::varchar(80) as organization_l2_name,
    organization_l3_code::varchar(18) as organization_l3_code,
    organization_l3_name::varchar(80) as organization_l3_name,
    organization_l4_code::varchar(18) as organization_l4_code,
    organization_l4_name::varchar(80) as organization_l4_name,
    organization_l5_code::varchar(18) as organization_l5_code,
    organization_l5_name::varchar(80) as organization_l5_name,
    organization_l6_code::varchar(18) as organization_l6_code,
    organization_l6_name::varchar(80) as organization_l6_name,
    organization_l7_code::varchar(18) as organization_l7_code,
    organization_l7_name::varchar(80) as organization_l7_name,
    organization_l8_code::varchar(18) as organization_l8_code,
    organization_l8_name::varchar(80) as organization_l8_name,
    organization_l9_code::varchar(18) as organization_l9_code,
    organization_l9_name::varchar(80) as organization_l9_name,
    common_organization_l1_code::varchar(18) as common_organization_l1_code,
    common_organization_l1_name::varchar(80) as common_organization_l1_name,
    common_organization_l2_code::varchar(18) as common_organization_l2_code,
    common_organization_l2_name::varchar(80) as common_organization_l2_name,
    common_organization_l3_code::varchar(18) as common_organization_l3_code,
    common_organization_l3_name::varchar(80) as common_organization_l3_name,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    msl_primary_responsible_ta::varchar(255) as msl_primary_responsible_ta,
    msl_secondary_responsible_ta::varchar(255) as msl_secondary_responsible_ta
 from final