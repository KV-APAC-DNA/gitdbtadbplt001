{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE ORGANIZATION_KEY ='Not Applicable';
        {% endif %}",
        post_hook = ["UPDATE {{this}}
        SET FLAG = 'HT',
        EFFECTIVE_TO_DATE = to_date(convert_timezone('UTC',current_timestamp())) - 1
        WHERE (TERRITORY_SOURCE_ID,COUNTRY_CODE) IN 
        (SELECT WRK.TERRITORY_SOURCE_ID,WRK.COUNTRY_CODE FROM {{this}} WRK 
        WHERE  EFFECTIVE_FROM_DATE = to_date(convert_timezone('UTC',current_timestamp())) )
        and to_date(EFFECTIVE_FROM_DATE) < ( select max(to_date(AA.EFFECTIVE_FROM_DATE)) EFFECTIVE_FROM_DATE FROM  {{this}} AA )
        AND   EFFECTIVE_TO_DATE=  to_date('12/31/2099','mm/dd/yyyy') ;","UPDATE {{this}} SET EFFECTIVE_FROM_DATE = '1900-01-01 00:00:00' 
        WHERE EFFECTIVE_FROM_DATE <> '1900-01-01 00:00:00' AND MY_ORGANIZATION_CODE IN 
        (SELECT MY_ORGANIZATION_CODE FROM {{this}} 
        WHERE MY_ORGANIZATION_CODE <> 'Not Applicable' GROUP BY 1 HAVING COUNT(MY_ORGANIZATION_CODE)=1);"]
    )
}}

with edw_hcp360_veeva_wrk_dim_organization as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_wrk_dim_organization') }}
),
final as
(
    SELECT 
    MD5(NEW_REC.TERRITORY_SOURCE_ID ||NEW_REC.COUNTRY_CODE || convert_timezone('UTC',current_timestamp())) AS ORGANIZATION_KEY,
    NEW_REC.TERRITORY_SOURCE_ID ,
    NEW_REC.COUNTRY_CODE ,
    NEW_REC.MY_ORGANIZATION_CODE,
    NEW_REC.MY_ORGANIZATION_NAME,
    NEW_REC.ORGANIZATION_L1_CODE,
    NEW_REC.ORGANIZATION_L1_NAME,
    NEW_REC.ORGANIZATION_L2_CODE,
    NEW_REC.ORGANIZATION_L2_NAME,
    NEW_REC.ORGANIZATION_L3_CODE,
    NEW_REC.ORGANIZATION_L3_NAME,
    NEW_REC.ORGANIZATION_L4_CODE,
    NEW_REC.ORGANIZATION_L4_NAME,
    NEW_REC.ORGANIZATION_L5_CODE,
    NEW_REC.ORGANIZATION_L5_NAME,
    NEW_REC.ORGANIZATION_L6_CODE,
    NEW_REC.ORGANIZATION_L6_NAME,
    NEW_REC.ORGANIZATION_L7_CODE,
    NEW_REC.ORGANIZATION_L7_NAME,
    NEW_REC.ORGANIZATION_L8_CODE,
    NEW_REC.ORGANIZATION_L8_NAME,
    NEW_REC.ORGANIZATION_L9_CODE,
    NEW_REC.ORGANIZATION_L9_NAME,
    convert_timezone('UTC',current_timestamp()) as crt_dttm,
    convert_timezone('UTC',current_timestamp()) as updt_dttm,
    convert_timezone('UTC',current_timestamp()) as effective_from_date,
    to_date('12/31/2099','mm/dd/yyyy') as effective_to_date,
    'NW' as FLAG
    from   
    (--- new records for org hierarchy
    select TERRITORY_SOURCE_ID, 
    COUNTRY_CODE,
    MY_ORGANIZATION_CODE,
    MY_ORGANIZATION_NAME,
    ORGANIZATION_L1_CODE,
    ORGANIZATION_L1_NAME,  
    ORGANIZATION_L2_CODE,
    ORGANIZATION_L2_NAME,
    nullif(ORGANIZATION_L3_CODE,'') as ORGANIZATION_L3_CODE,
    nullif(ORGANIZATION_L3_NAME,'') as ORGANIZATION_L3_NAME,
    nullif(ORGANIZATION_L4_CODE,'') as ORGANIZATION_L4_CODE,
    nullif(ORGANIZATION_L4_NAME,'') as ORGANIZATION_L4_NAME,
    nullif(ORGANIZATION_L5_CODE,'') as ORGANIZATION_L5_CODE,
    nullif(ORGANIZATION_L5_NAME,'') as ORGANIZATION_L5_NAME,
    nullif(ORGANIZATION_L6_CODE,'') as ORGANIZATION_L6_CODE,
    nullif(ORGANIZATION_L6_NAME,'') as ORGANIZATION_L6_NAME,
    nullif(ORGANIZATION_L7_CODE,'') as ORGANIZATION_L7_CODE,
    nullif(ORGANIZATION_L7_NAME,'') as ORGANIZATION_L7_NAME,
    nullif(ORGANIZATION_L8_CODE,'') as ORGANIZATION_L8_CODE,
    nullif(ORGANIZATION_L8_NAME,'') as ORGANIZATION_L8_NAME,
    nullif(ORGANIZATION_L9_CODE,'') as ORGANIZATION_L9_CODE,
    nullif(ORGANIZATION_L9_NAME,'') as ORGANIZATION_L9_NAME
    from edw_hcp360_veeva_wrk_dim_organization
    MINUS
    select  TERRITORY_SOURCE_ID,
    COUNTRY_CODE,
    MY_ORGANIZATION_CODE,
    MY_ORGANIZATION_NAME,
    ORGANIZATION_L1_CODE,
    ORGANIZATION_L1_NAME,  
    ORGANIZATION_L2_CODE,
    ORGANIZATION_L2_NAME,
    nullif(ORGANIZATION_L3_CODE,''),
    nullif(ORGANIZATION_L3_NAME,''),
    nullif(ORGANIZATION_L4_CODE,''),
    nullif(ORGANIZATION_L4_NAME,''),
    nullif(ORGANIZATION_L5_CODE,''),
    nullif(ORGANIZATION_L5_NAME,''),
    nullif(ORGANIZATION_L6_CODE,''),
    nullif(ORGANIZATION_L6_NAME,''),
    nullif(ORGANIZATION_L7_CODE,''),
    nullif(ORGANIZATION_L7_NAME,''),
    nullif(ORGANIZATION_L8_CODE,''),
    nullif(ORGANIZATION_L8_NAME,''),
    nullif(ORGANIZATION_L9_CODE,''),
    nullif(ORGANIZATION_L9_NAME,'')
    from {{this}}
    ) NEW_REC 

    UNION ALL
    -- DUMMY RECORDS
    SELECT  'Not Applicable', 'Not Applicable', 'ZZ', 'Not Applicable','Not Applicable','Not Applicable', 'Not Applicable', 'Not Applicable',
            'Not Applicable','Not Applicable',	'Not Applicable', 'Not Applicable','Not Applicable', 'Not Applicable', 'Not Applicable','Not Applicable',
            'Not Applicable','Not Applicable','Not Applicable', 'Not Applicable','Not Applicable','Not Applicable','Not Applicable',	
            convert_timezone('UTC',current_timestamp()),	convert_timezone('UTC',current_timestamp()), 	convert_timezone('UTC',current_timestamp()),	convert_timezone('UTC',current_timestamp()) , 'Not Applicable'
)
select organization_key::varchar(32) as organization_key,
    territory_source_id::varchar(18) as territory_source_id,
    country_code::varchar(8) as country_code,
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
    effective_from_date::date as effective_from_date,
    effective_to_date::date as effective_to_date,
    flag::varchar(18) as flag,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final