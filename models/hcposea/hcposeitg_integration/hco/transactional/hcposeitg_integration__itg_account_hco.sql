
{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook="DELETE FROM {{this}}
                    WHERE ACCOUNT_SOURCE_ID IN (SELECT ACCOUNT_SOURCE_ID
                                                FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_account_hco') }} STG_HCO
                                                WHERE STG_HCO.ACCOUNT_SOURCE_ID = ACCOUNT_SOURCE_ID)
                    AND   COUNTRY_CODE IN (SELECT DISTINCT UPPER(COUNTRY_CODE)
                                        FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_account_hco') }});",
                        
        post_hook=" UPDATE {{this}} ITG_ACCOUNT_HCO
            SET PARENT_HCO_NAME = STG.HCO_NAME
            FROM (SELECT STG_HCO.ACCOUNT_SOURCE_ID,
                        UPPER(STG_HCO.COUNTRY_CODE) COUNTRY_CODE,
                        STG_HCO.HCO_NAME,
                        ITG_HCO.PRIMARY_PARENT_SOURCE_ID
                FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_account_hco') }} STG_HCO
                JOIN {{this}} ITG_HCO ON ITG_HCO.PRIMARY_PARENT_SOURCE_ID = STG_HCO.ACCOUNT_SOURCE_ID
                AND ITG_HCO.COUNTRY_CODE = STG_HCO.COUNTRY_CODE) STG
            WHERE ITG_ACCOUNT_HCO.PRIMARY_PARENT_SOURCE_ID = STG.PRIMARY_PARENT_SOURCE_ID
            AND   ITG_ACCOUNT_HCO.COUNTRY_CODE = STG.COUNTRY_CODE;"
                            )  }}


WITH 
sdl_hcp_osea_account_hco as
(
    select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_account_hco') }}
),
t1
AS (
    SELECT MD5(UPPER(COUNTRY_CODE) || ACCOUNT_SOURCE_ID) AS HCO_KEY,
        (
            CASE 
                WHEN PRIMARY_PARENT_SOURCE_ID IS NULL
                    THEN MD5(UPPER(COUNTRY_CODE) || ACCOUNT_SOURCE_ID)
                WHEN PRIMARY_PARENT_SOURCE_ID = ' '
                    THEN MD5(UPPER(COUNTRY_CODE) || ACCOUNT_SOURCE_ID)
                ELSE MD5(UPPER(COUNTRY_CODE) || PRIMARY_PARENT_SOURCE_ID)
                END
            ) AS PARENT_HCO_KEY,
        ACCOUNT_SOURCE_ID,
        (
            CASE 
                WHEN UPPER(IS_DELETED) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_DELETED) IS NULL
                    THEN 0
                WHEN UPPER(IS_DELETED) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS IS_DELETED,
        ACCOUNT_NAME,
        RECORD_TYPE_SOURCE_ID,
        PHONE_NUMBER,
        FAX_NUMBER,
        WEBSITE,
        OWNER_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        (
            CASE 
                WHEN UPPER(IS_EXCLUDED_FROM_REALIGN) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_EXCLUDED_FROM_REALIGN) IS NULL
                    THEN 0
                WHEN UPPER(IS_EXCLUDED_FROM_REALIGN) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS IS_EXCLUDED_FROM_REALIGN,
        IS_PERSON_ACCOUNT,
        EXTERNAL_ID,
        TERRITORY_NAME,
        BEDS,
        PRIMARY_PARENT_SOURCE_ID,
        TOTAL_MDS_DOS,
        DEPARTMENTS,
        HCO_TYPE,
        HCO_Sector,
        (
            CASE 
                WHEN UPPER(SFE_APPROVED) = 'TRUE'
                    THEN 1
                WHEN UPPER(SFE_APPROVED) IS NULL
                    THEN 0
                WHEN UPPER(SFE_APPROVED) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS SFE_APPROVED,
        UPPER(COUNTRY_CODE) as COUNTRY_CODE,
        BUSINESS_DESCRIPTION,
        (
            CASE 
                WHEN UPPER(HCC_ACCOUNT_APPROVED) = 'TRUE'
                    THEN 1
                WHEN UPPER(HCC_ACCOUNT_APPROVED) IS NULL
                    THEN 0
                WHEN UPPER(HCC_ACCOUNT_APPROVED) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS HCC_ACCOUNT_APPROVED,
        (
            CASE 
                WHEN UPPER(INACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(INACTIVE) IS NULL
                    THEN 0
                WHEN UPPER(INACTIVE) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS INACTIVE,
        TOTAL_PHYSICIANS_ENROLLED,
        TOTAL_PHARMACISTS,
        (
            CASE 
                WHEN UPPER(IS_EXTERNAL_ID_NUMBER) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_EXTERNAL_ID_NUMBER) IS NULL
                    THEN 0
                WHEN UPPER(IS_EXTERNAL_ID_NUMBER) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS IS_EXTERNAL_ID_NUMBER,
        OT,
        KAM_PAEDIATRIC,
        KAM_OBGYN,
        KAM_MINIMALLY_INVASIVE,
        KAM_TOTAL_UROLOGYSURGEONS,
        KAM_TOTAL_SURGEONS,
        KAM_TOTAL_RHEUMPHYSICIANS,
        KAM_TOTAL_PSYCHIATRYPHYSICIANS,
        KAM_TOTAL_ORTHOSURGEONS,
        KAM_TOTAL_OPTHALSURGEONS,
        KAM_TOTAL_NEUROLOGYPHYSICIANS,
        KAM_TOTAL_MEDONCOPHYSICIANS,
        KAM_TOTAL_INFECTIOUSPHYSICIANS,
        KAM_TOTAL_HAEMAPHYSICIANS,
        KAM_TOTAL_GENERALSURGEONS,
        KAM_TOTAL_GASTROPHYSICIANS,
        KAM_TOTAL_ENDOPHYSICIANS,
        KAM_TOTAL_DERMAPHYSICIANS,
        KAM_TOTAL_CARDIOLOGYPHYSICIANS,
        KAM_TOTAL_CARDIOSURGEONS,
        KAM_TOTAL_AESTHETICSURGEONS,
        KAM_GENERAL_DIFFERNENTIATIONS,
        KAM_CLINICAL_DIFFERENTIATIONS,
        HCO_NAME,
        REMARKS,
        (
            CASE 
                WHEN PRIMARY_PARENT_SOURCE_ID IS NULL
                    THEN HCO_NAME
                WHEN PRIMARY_PARENT_SOURCE_ID = ' '
                    THEN HCO_NAME
                ELSE NULL
                END
            ) AS PARENT_HCO_NAME,
        TW_CUSTOMER_CODE as CUSTOMER_CODE,
        current_timestamp() as inserted_date,
        NULL as updated_date,
        jj_external_id,
        specialty_2,
        sub_specialty,
        sea_account_classification,
        jj_email_1,
        jj_email_2
    FROM sdl_hcp_OSEA_ACCOUNT_HCO
    ),
final
AS (
    SELECT HCO_KEY::VARCHAR(32) as HCO_KEY,
	PARENT_HCO_KEY::VARCHAR(32) as PARENT_HCO_KEY,
	ACCOUNT_SOURCE_ID::VARCHAR(18) as ACCOUNT_SOURCE_ID,
	IS_DELETED::NUMBER(38,0) as IS_DELETED,
	ACCOUNT_NAME::VARCHAR(255) as ACCOUNT_NAME,
	RECORD_TYPE_SOURCE_ID::VARCHAR(18) as RECORD_TYPE_SOURCE_ID,
	PHONE_NUMBER::VARCHAR(40) as PHONE_NUMBER,
	FAX_NUMBER::VARCHAR(40) as FAX_NUMBER,
	WEBSITE::VARCHAR(255) as WEBSITE,
	OWNER_ID::VARCHAR(18) as OWNER_ID,
	CREATED_DATE::TIMESTAMP_NTZ(9) as CREATED_DATE,
	CREATED_BY_ID::VARCHAR(18) as CREATED_BY_ID,
	LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) as LAST_MODIFIED_DATE,
	LAST_MODIFIED_BY_ID::VARCHAR(18) as LAST_MODIFIED_BY_ID,
	IS_EXCLUDED_FROM_REALIGN::NUMBER(38,0) as IS_EXCLUDED_FROM_REALIGN,
	IS_PERSON_ACCOUNT::VARCHAR(5) as IS_PERSON_ACCOUNT,
	EXTERNAL_ID::VARCHAR(120) as EXTERNAL_ID,
	TERRITORY_NAME::VARCHAR(255) as TERRITORY_NAME,
	BEDS::NUMBER(4,0) as BEDS,
	PRIMARY_PARENT_SOURCE_ID::VARCHAR(18) as PRIMARY_PARENT_SOURCE_ID,
	TOTAL_MDS_DOS::NUMBER(18,0) as TOTAL_MDS_DOS,
	DEPARTMENTS::NUMBER(18,0) as DEPARTMENTS,
	HCO_TYPE::VARCHAR(255) as HCO_TYPE,
	HCO_SECTOR::VARCHAR(255) as HCO_SECTOR,
	SFE_APPROVED::NUMBER(38,0) as SFE_APPROVED,
	COUNTRY_CODE::VARCHAR(8) as COUNTRY_CODE,
	BUSINESS_DESCRIPTION::VARCHAR(32000) as BUSINESS_DESCRIPTION,
	HCC_ACCOUNT_APPROVED::NUMBER(38,0) as HCC_ACCOUNT_APPROVED,
	INACTIVE::NUMBER(38,0) as INACTIVE,
	TOTAL_PHYSICIANS_ENROLLED::NUMBER(18,0) as TOTAL_PHYSICIANS_ENROLLED,
	TOTAL_PHARMACISTS::NUMBER(3,0) as TOTAL_PHARMACISTS,
	IS_EXTERNAL_ID_NUMBER::NUMBER(38,0) as IS_EXTERNAL_ID_NUMBER,
	OT::NUMBER(8,0) as OT,
	KAM_PAEDIATRIC::NUMBER(18,0) as KAM_PAEDIATRIC,
	KAM_OBGYN::NUMBER(18,0) as KAM_OBGYN,
	KAM_MINIMALLY_INVASIVE::NUMBER(18,0) as KAM_MINIMALLY_INVASIVE,
	KAM_TOTAL_UROLOGYSURGEONS::NUMBER(18,0) as KAM_TOTAL_UROLOGYSURGEONS,
	KAM_TOTAL_SURGEONS::NUMBER(18,0) as KAM_TOTAL_SURGEONS,
	KAM_TOTAL_RHEUMPHYSICIANS::NUMBER(18,0) as KAM_TOTAL_RHEUMPHYSICIANS,
	KAM_TOTAL_PSYCHIATRYPHYSICIANS::NUMBER(18,0) as KAM_TOTAL_PSYCHIATRYPHYSICIANS,
	KAM_TOTAL_ORTHOSURGEONS::NUMBER(18,0) as KAM_TOTAL_ORTHOSURGEONS,
	KAM_TOTAL_OPTHALSURGEONS::NUMBER(18,0) as KAM_TOTAL_OPTHALSURGEONS,
	KAM_TOTAL_NEUROLOGYPHYSICIANS::NUMBER(18,0) as KAM_TOTAL_NEUROLOGYPHYSICIANS,
	KAM_TOTAL_MEDONCOPHYSICIANS::NUMBER(18,0) as KAM_TOTAL_MEDONCOPHYSICIANS,
	KAM_TOTAL_INFECTIOUSPHYSICIANS::NUMBER(18,0) as KAM_TOTAL_INFECTIOUSPHYSICIANS,
	KAM_TOTAL_HAEMAPHYSICIANS::NUMBER(18,0) as KAM_TOTAL_HAEMAPHYSICIANS,
	KAM_TOTAL_GENERALSURGEONS::NUMBER(18,0) as KAM_TOTAL_GENERALSURGEONS,
	KAM_TOTAL_GASTROPHYSICIANS::NUMBER(18,0) as KAM_TOTAL_GASTROPHYSICIANS,
	KAM_TOTAL_ENDOPHYSICIANS::NUMBER(18,0) as KAM_TOTAL_ENDOPHYSICIANS,
	KAM_TOTAL_DERMAPHYSICIANS::NUMBER(18,0) as KAM_TOTAL_DERMAPHYSICIANS,
	KAM_TOTAL_CARDIOLOGYPHYSICIANS::NUMBER(18,0) as KAM_TOTAL_CARDIOLOGYPHYSICIANS,
	KAM_TOTAL_CARDIOSURGEONS::NUMBER(18,0) as KAM_TOTAL_CARDIOSURGEONS,
	KAM_TOTAL_AESTHETICSURGEONS::NUMBER(18,0) as KAM_TOTAL_AESTHETICSURGEONS,
	KAM_GENERAL_DIFFERNENTIATIONS::VARCHAR(32768) as KAM_GENERAL_DIFFERNENTIATIONS,
	KAM_CLINICAL_DIFFERENTIATIONS::VARCHAR(32768) as KAM_CLINICAL_DIFFERENTIATIONS,
	HCO_NAME::VARCHAR(1300) as HCO_NAME,
	REMARKS::VARCHAR(255) as REMARKS,
	PARENT_HCO_NAME::VARCHAR(255) as PARENT_HCO_NAME,
	CUSTOMER_CODE::VARCHAR(60) as CUSTOMER_CODE,
	INSERTED_DATE::TIMESTAMP_NTZ(9) as INSERTED_DATE,
	UPDATED_DATE::TIMESTAMP_NTZ(9) as UPDATED_DATE,
	JJ_EXTERNAL_ID::VARCHAR(90) as JJ_EXTERNAL_ID,
	SPECIALTY_2::VARCHAR(255) as SPECIALTY_2,
	SUB_SPECIALTY::VARCHAR(255) as SUB_SPECIALTY,
	SEA_ACCOUNT_CLASSIFICATION::VARCHAR(255) as SEA_ACCOUNT_CLASSIFICATION,
	JJ_EMAIL_1::VARCHAR(80) as JJ_EMAIL_1,
	JJ_EMAIL_2::VARCHAR(80) as JJ_EMAIL_2
    FROM t1
    )
SELECT *
FROM final