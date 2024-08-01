{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where source_system = 'IQVIA'
        and country = 'IN';
        {% endif %}"
    )
}}
with 
edw_hcp360_in_iqvia_sales as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_iqvia_sales') }}
),
itg_hcp360_in_iqvia_brand as 
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_in_iqvia_brand') }}
),
itg_hcp360_in_iqvia_speciality as 
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_in_iqvia_speciality') }}
),
itg_hcp360_in_iqvia_indication as 
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_iqvia_indication') }}
),
itg_mds_hcp360_product_mapping as 
(
    select * from {{ ref('hcpitg_integration__itg_mds_hcp360_product_mapping') }}
),
wks_hcp_360_iqvia_brand_orsl_mat_temp as 
(
    select * from {{ ref('hcpwks_integration__wks_hcp_360_iqvia_brand_orsl_mat_temp') }}
),
wks_hcp_360_iqvia_brand_derma_mat_temp as 
(
    select * from {{ ref('hcpwks_integration__wks_hcp_360_iqvia_brand_derma_mat_temp') }}
),
wks_hcp_360_iqvia_brand_aveenobaby_mat_temp as 
(
    select * from {{ ref('hcpwks_integration__wks_hcp_360_iqvia_brand_aveenobaby_mat_temp') }}
),
wks_hcp_360_iqvia_brand_jbaby_mat_temp as 
(
    select * from {{ ref('hcpwks_integration__wks_hcp_360_iqvia_brand_jbaby_mat_temp') }}
),

temp1 as 
(
    with BASE as (
        SELECT 'IN'::varchar AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            iqvia.data_source AS datasource,
            iqvia.brand_category AS BRAND_CATEGORY,
            iqvia.Report_Brand As REPORT_BRAND_REFERENCE ,
            'ORSL Total'::varchar as IQVIA_BRAND,
            iqvia.ZONE AS REGION,
            iqvia.ACTIVITY_DATE,
            iqvia.pack_description,
            iqvia.product_description,
            iqvia.input_brand,
            iqvia.no_of_prescriptions AS NoofPrescritions,
            iqvia.no_of_prescribers AS NoofPrescribers,
            derived.MAT_NoofPrescritions,
            derived.MAT_NoofPrescribers,
            sum(derived.MAT_NoofPrescritions) over (
                partition by Report_Brand,
                iqvia.brand_category,
                activity_date
            ) as MAT_TotalPrescritions_by_Brand,
            sum(derived.MAT_NoofPrescribers) over (
                partition by Report_Brand,
                iqvia.brand_category,
                activity_date
            ) as MAT_TotalPrescribers_by_Brand,
            sum(iqvia.no_of_prescriptions) over (
                partition by Report_Brand,
                iqvia.brand_category,
                activity_date
            ) as TotalPrescritions_by_Brand,
            sum(iqvia.no_of_prescribers) over (
                partition by Report_Brand,
                iqvia.brand_category,
                activity_date
            ) as TotalPrescribers_by_Brand,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT nvl(M.BRAND, I.BRAND || '_COMP') as Report_Brand,
                    I.product_description,
                    I.brand as input_brand,
                    I.pack_description,
                    I.pack_form,
                    I.brand_category,
                    I.zone,
                    cast(I.Year_month as Date) AS ACTIVITY_DATE,
                    I.no_of_prescriptions,
                    I.no_of_prescribers,
                    I.data_source,
                    I.pack_volume
                FROM ITG_HCP360_IN_IQVIA_BRAND I,
(
                        select *
                        from ITG_MDS_HCP360_PRODUCT_MAPPING
                        where Brand = 'ORSL'
                    ) M
                WHERE I.PACK_DESCRIPTION = M.IQVIA(+)
                    and I.data_source = 'ORSL'
            ) iqvia
            left join wks_hcp_360_iqvia_brand_orsl_mat_temp derived ON iqvia.brand_category = derived.brand_category
            AND iqvia.report_brand = derived.report_brand_reference --AND   iqvia.iqvia_brand             =  derived.iqvia_brand
            AND iqvia.zone = derived.region
            AND iqvia.Product_Description = derived.Product_Description
            AND iqvia.Pack_Description = derived.Pack_Description
            AND iqvia.input_Brand = derived.Brand
            AND iqvia.ACTIVITY_DATE = derived.activity_till_date
    ),
    REPORT_BRAND as (
        SELECT DISTINCT country,
            source_system,
            datasource,
            brand_category,
            report_brand_reference,
            iqvia_brand,
            activity_date,
            mat_totalprescritions_by_brand,
            TotalPrescritions_by_Brand
        FROM BASE
        WHERE report_brand_reference not like '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    NULL AS ACTIVITY_TYPE --Check KPI Sheet			
,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.datasource,
    BASE.brand_category,
    BASE.report_brand_reference,
    BASE.iqvia_brand,
    NULL AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    NULL AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    BASE.pack_description,
    BASE.product_description,
    BASE.input_brand,
    BASE.noofprescritions,
    BASE.noofprescribers,
    BASE.mat_noofprescritions,
    BASE.mat_noofprescribers,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.mat_totalprescritions_by_brand
    end as mat_totalprescritions_by_brand,
    BASE.mat_totalprescribers_by_brand,
    REPORT_BRAND.mat_totalprescritions_by_brand as mat_totalprescritions_jnj_brand,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.TotalPrescritions_by_Brand
    end as TotalPrescritions_by_Brand,
    BASE.TotalPrescribers_by_Brand,
    REPORT_BRAND.TotalPrescritions_by_Brand as totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.datasource = REPORT_BRAND.datasource(+)
    AND BASE.brand_category = REPORT_BRAND.brand_category (+)
    AND BASE.iqvia_brand = REPORT_BRAND.iqvia_brand (+) --AND BASE.report_brand_reference = REPORT_BRAND. report_brand_reference (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
temp2 as 
(
    with BASE as (
        SELECT 'IN'::varchar AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            'DERMA'::varchar AS BRAND,
            iqvia.brand_category AS BRAND_CATEGORY,
            iqvia.Report_Brand As REPORT_BRAND_REFERENCE --,case when Report_Brand like'%COMP' then NULL else 'DERMA Total' end  AS IQVIA_BRAND 
,
            'AVEENO BODY'::varchar AS IQVIA_BRAND,
            iqvia.ZONE AS REGION,
            iqvia.ACTIVITY_DATE,
            iqvia.product_description,
            iqvia.pack_description,
            iqvia.pack_volume,
            iqvia.no_of_prescriptions AS NoofPrescritions,
            iqvia.no_of_prescribers AS NoofPrescribers,
            derived.MAT_NoofPrescritions,
            derived.MAT_NoofPrescribers,
            sum(derived.MAT_NoofPrescritions) over (partition by Report_Brand, activity_date) as MAT_TotalPrescritions_by_Brand,
            sum(derived.MAT_NoofPrescribers) over (partition by Report_Brand, activity_date) as MAT_TotalPrescribers_by_Brand,
            sum(iqvia.no_of_prescriptions) over (partition by Report_Brand, activity_date) as TotalPrescritions_by_Brand,
            sum(iqvia.no_of_prescribers) over (partition by Report_Brand, activity_date) as TotalPrescribers_by_Brand,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT 'DERMA' as Brand,
                    nvl(
                        M.BRAND,
case
                            when position(' ' in I.Product_description) > 0 then split_part(I.Product_description, ' ', 1)
                            when position('-' in I.Product_description) > 0 then split_part(I.Product_description, '-', 1)
                            else I.Product_description
                        end || '_COMP'
                    ) as Report_Brand,
                    product_description,
                    pack_description,
                    pack_form,
                    brand_category,
                    zone,
                    cast(Year_month as Date) AS activity_date,
                    no_of_prescriptions,
                    no_of_prescribers,
                    pack_volume
                FROM ITG_HCP360_IN_IQVIA_BRAND I,
(
                        select case
                                when split_part(iqvia, ' ', 1) = 'AVEENO' then 'AVEENO BODY'
                                else brand
                            end as brand,
                            iqvia
                        from ITG_MDS_HCP360_PRODUCT_MAPPING
                        where brand = 'DERMA'
                    ) M
                WHERE I.PACK_DESCRIPTION = M.IQVIA(+)
                    and I.data_source = 'Aveeno_body'
            ) iqvia
            left join wks_hcp_360_iqvia_brand_derma_mat_temp derived ON iqvia.brand = derived.brand
            AND iqvia.report_brand = derived.report_brand_reference
            AND iqvia.zone = derived.region
            AND iqvia.Product_Description = derived.Product_Description
            AND iqvia.Pack_Description = derived.Pack_Description
            AND iqvia.pack_volume = derived.pack_volume
            AND iqvia.activity_date = derived.activity_till_date
    ),
    REPORT_BRAND as (
        SELECT DISTINCT country,
            source_system,
            brand,
            report_brand_reference,
            iqvia_brand,
            activity_date,
            mat_totalprescritions_by_brand,
            totalprescritions_by_brand
        FROM BASE
        WHERE report_brand_reference not like '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    NULL AS ACTIVITY_TYPE --Check KPI Sheet			
,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.brand,
    BASE.brand_category,
    BASE.report_brand_reference,
    BASE.iqvia_brand,
    NULL AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    NULL AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    BASE.product_description,
    BASE.pack_description,
    BASE.pack_volume,
    BASE.noofprescritions,
    BASE.noofprescribers,
    BASE.mat_noofprescritions,
    BASE.mat_noofprescribers,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.mat_totalprescritions_by_brand
    end as mat_totalprescritions_by_brand --,	BASE.mat_totalprescritions_by_brand
,
    BASE.mat_totalprescribers_by_brand,
    REPORT_BRAND.mat_totalprescritions_by_brand as mat_totalprescritions_jnj_brand,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.totalprescritions_by_brand
    end as totalprescritions_by_brand,
    BASE.totalprescribers_by_brand,
    REPORT_BRAND.totalprescritions_by_brand as totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.brand = REPORT_BRAND.brand (+)
    AND BASE.iqvia_brand = REPORT_BRAND.iqvia_brand (+) --AND BASE.report_brand_reference = REPORT_BRAND. report_brand_reference (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
temp3 as 
(
    WITH BASE AS (
        SELECT 'IN'::varchar AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            'DERMA'::varchar AS BRAND,
            iqvia.brand_category AS BRAND_CATEGORY,
            iqvia.Report_Brand As REPORT_BRAND_REFERENCE --,case when Report_Brand like'%COMP' then NULL else 'JBABY Total' end AS IQVIA_BRAND	
,
            'AVEENO BABY'::varchar as IQVIA_BRAND,
            iqvia.ZONE AS REGION,
            iqvia.ACTIVITY_DATE,
            iqvia.product_description,
            iqvia.pack_description,
            iqvia.pack_volume,
            iqvia.no_of_prescriptions AS NoofPrescritions,
            iqvia.no_of_prescribers AS NoofPrescribers,
            derived.MAT_NoofPrescritions,
            derived.MAT_NoofPrescribers,
            sum(derived.MAT_NoofPrescritions) over (partition by Report_Brand, activity_date) as MAT_TotalPrescritions_by_Brand,
            sum(derived.MAT_NoofPrescribers) over (partition by Report_Brand, activity_date) as MAT_TotalPrescribers_by_Brand,
            sum(iqvia.no_of_prescriptions) over (partition by Report_Brand, activity_date) as TotalPrescritions_by_Brand,
            sum(iqvia.no_of_prescribers) over (partition by Report_Brand, activity_date) as TotalPrescribers_by_Brand,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT 'DERMA' as Brand,
                    nvl(
                        M.Brand,
case
                            when position(' ' in I.Product_description) > 0 then split_part(I.Product_description, ' ', 1)
                            when position('-' in I.Product_description) > 0 then split_part(I.Product_description, '-', 1)
                            else I.Product_description
                        end || '_COMP'
                    ) as Report_Brand,
                    product_description,
                    pack_description,
                    pack_form,
                    brand_category,
                    zone,
                    cast(Year_month as Date) AS activity_date,
                    no_of_prescriptions,
                    no_of_prescribers as no_of_prescribers,
                    pack_volume
                FROM ITG_HCP360_IN_IQVIA_BRAND I,
(
                        select *
                        from ITG_MDS_HCP360_PRODUCT_MAPPING
                        where Brand in ('AVEENO BABY')
                    ) M
                WHERE I.PACK_DESCRIPTION = M.IQVIA(+)
                    and I.data_source = 'Aveeno_baby'
                    and split_part(I.Product_description, ' ', 1) != 'JOHNSON' --and upper(pack_description) like '%BABY%'
            ) iqvia
            left join wks_hcp_360_iqvia_brand_aveenobaby_mat_temp derived on iqvia.brand = derived.brand
            AND iqvia.report_brand = derived.report_brand_reference
            AND iqvia.zone = derived.region
            AND iqvia.Product_Description = derived.Product_Description
            AND iqvia.Pack_Description = derived.Pack_Description
            AND iqvia.pack_volume = derived.pack_volume
            AND iqvia.activity_date = derived.activity_till_date
    ),
    REPORT_BRAND as (
        SELECT DISTINCT country,
            source_system,
            brand,
            report_brand_reference,
            iqvia_brand,
            activity_date,
            mat_totalprescritions_by_brand,
            totalprescritions_by_brand
        FROM BASE
        WHERE report_brand_reference not like '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    NULL AS ACTIVITY_TYPE --Check KPI Sheet			
,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.brand,
    BASE.brand_category,
    BASE.report_brand_reference,
    BASE.iqvia_brand,
    NULL AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    NULL AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    BASE.product_description,
    BASE.pack_description,
    BASE.pack_volume,
    BASE.noofprescritions,
    BASE.noofprescribers,
    BASE.mat_noofprescritions,
    BASE.mat_noofprescribers,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.mat_totalprescritions_by_brand
    end as mat_totalprescritions_by_brand --,	BASE.mat_totalprescritions_by_brand
,
    BASE.mat_totalprescribers_by_brand,
    REPORT_BRAND.mat_totalprescritions_by_brand as mat_totalprescritions_jnj_brand,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.totalprescritions_by_brand
    end as totalprescritions_by_brand,
    BASE.totalprescribers_by_brand,
    REPORT_BRAND.totalprescritions_by_brand as totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.brand = REPORT_BRAND.brand (+)
    AND BASE.iqvia_brand = REPORT_BRAND.iqvia_brand (+) --AND BASE.report_brand_reference = REPORT_BRAND. report_brand_reference (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
temp4 as 
(
    WITH BASE AS (
        SELECT 'IN'::varchar AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            'JBABY'::varchar AS BRAND,
            iqvia.brand_category AS BRAND_CATEGORY,
            iqvia.Report_Brand As REPORT_BRAND_REFERENCE --,case when Report_Brand like'%COMP' then NULL else 'JBABY Total' end AS IQVIA_BRAND	
,
            'JBABY Total'::varchar as IQVIA_BRAND,
            iqvia.ZONE AS REGION,
            iqvia.ACTIVITY_DATE,
            iqvia.product_description,
            iqvia.pack_description,
            iqvia.pack_volume,
            iqvia.no_of_prescriptions AS NoofPrescritions,
            iqvia.no_of_prescribers AS NoofPrescribers,
            derived.MAT_NoofPrescritions,
            derived.MAT_NoofPrescribers,
            sum(derived.MAT_NoofPrescritions) over (partition by Report_Brand, activity_date) as MAT_TotalPrescritions_by_Brand,
            sum(derived.MAT_NoofPrescribers) over (partition by Report_Brand, activity_date) as MAT_TotalPrescribers_by_Brand,
            sum(iqvia.no_of_prescriptions) over (partition by Report_Brand, activity_date) as TotalPrescritions_by_Brand,
            sum(iqvia.no_of_prescribers) over (partition by Report_Brand, activity_date) as TotalPrescribers_by_Brand,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT 'JBABY' as Brand,
                    nvl(
                        M.Brand,
case
                            when position(' ' in I.Product_description) > 0 then split_part(I.Product_description, ' ', 1)
                            when position('-' in I.Product_description) > 0 then split_part(I.Product_description, '-', 1)
                            else I.Product_description
                        end || '_COMP'
                    ) as Report_Brand,
                    product_description,
                    pack_description,
                    pack_form,
                    brand_category,
                    zone,
                    cast(Year_month as Date) AS activity_date,
                    no_of_prescriptions,
                    no_of_prescribers as no_of_prescribers,
                    pack_volume
                FROM ITG_HCP360_IN_IQVIA_BRAND I,
(
                        select *
                        from ITG_MDS_HCP360_PRODUCT_MAPPING
                        where Brand in ('JBABY')
                    ) M
                WHERE I.PACK_DESCRIPTION = M.IQVIA(+)
                    and I.data_source = 'Aveeno_baby'
                    and split_part(I.Product_description, ' ', 1) != 'AVEENO'
                    and upper(pack_description) like '%BABY%'
            ) iqvia
            left join wks_hcp_360_iqvia_brand_jbaby_mat_temp derived on iqvia.brand = derived.brand
            AND iqvia.report_brand = derived.report_brand_reference
            AND iqvia.zone = derived.region
            AND iqvia.Product_Description = derived.Product_Description
            AND iqvia.Pack_Description = derived.Pack_Description
            AND iqvia.pack_volume = derived.pack_volume
            AND iqvia.activity_date = derived.activity_till_date
    ),
    REPORT_BRAND as (
        SELECT DISTINCT country,
            source_system,
            brand,
            report_brand_reference,
            iqvia_brand,
            activity_date,
            mat_totalprescritions_by_brand,
            totalprescritions_by_brand
        FROM BASE
        WHERE report_brand_reference not like '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    NULL AS ACTIVITY_TYPE --Check KPI Sheet			
,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.brand,
    BASE.brand_category,
    BASE.report_brand_reference,
    BASE.iqvia_brand,
    NULL AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    NULL AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    BASE.product_description,
    BASE.pack_description,
    BASE.pack_volume,
    BASE.noofprescritions,
    BASE.noofprescribers,
    BASE.mat_noofprescritions,
    BASE.mat_noofprescribers,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.mat_totalprescritions_by_brand
    end as mat_totalprescritions_by_brand,
    BASE.mat_totalprescribers_by_brand,
    REPORT_BRAND.mat_totalprescritions_by_brand as mat_totalprescritions_jnj_brand,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.totalprescritions_by_brand
    end as totalprescritions_by_brand,
    BASE.totalprescribers_by_brand,
    REPORT_BRAND.totalprescritions_by_brand as totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.brand = REPORT_BRAND.brand (+)
    AND BASE.iqvia_brand = REPORT_BRAND.iqvia_brand (+) --AND BASE.report_brand_reference = REPORT_BRAND. report_brand_reference (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
temp5 as 
(
    with BASE as (
        SELECT 'IN'::varchar AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            iqvia.data_source AS datasource,
            iqvia.Report_Brand AS REPORT_BRAND_REFERENCE,
            iqvia.Speciality_Report,
            iqvia.ZONE AS REGION,
            iqvia.ACTIVITY_DATE,
            iqvia.product_description,
            iqvia.input_brand,
            iqvia.no_of_prescriptions AS NoofPrescritions,
            iqvia.no_of_prescribers AS NoofPrescribers,
            SUM(iqvia.no_of_prescriptions) OVER (
                PARTITION BY Report_Brand,
                iqvia.Speciality_Report,
                activity_date
            ) AS TotalPrescritions_by_speciality,
            SUM(iqvia.no_of_prescribers) OVER (
                PARTITION BY Report_Brand,
                iqvia.Speciality_Report,
                activity_date
            ) AS TotalPrescribers_by_speciality,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT data_source,
                    ZONE,
                    product_description,
                    i.brand as input_brand,
                    cast(I.Year_month as Date) AS ACTIVITY_DATE,
                    no_of_prescriptions,
                    no_of_prescribers,
                    -- nvl(M.BRAND,I.BRAND||'_COMP') as Report_Brand ,
                    CASE
                        WHEN Product_Description = 'ORSL Total' THEN 'ORSL'
                        ELSE i.brand || '_COMP'
                    END AS Report_Brand,
                    Speciality_Report,
                    I.pack_volume
                FROM (
                        SELECT *,
                            CASE
                                WHEN speciality = 'GP-NON.MBBS' THEN 'GP - Non-MBBS'
                                WHEN speciality = 'GP - MBBS' THEN 'GP - MBBS'
                                WHEN speciality = 'PEDIATRICIAN' THEN 'Pediatrics'
                                WHEN speciality = 'GYNECOLOGIST' THEN 'Gynecology'
                                WHEN speciality = 'CONS. PHYSICIAN' THEN 'Consultation'
                                WHEN speciality NOT IN (
                                    'GP-NON.MBBS',
                                    'GP - MBBS',
                                    'PEDIATRICIAN',
                                    'GYNECOLOGIST',
                                    'CONS. PHYSICIAN'
                                ) THEN 'Others'
                            END AS Speciality_Report
                        FROM itg_hcp360_in_iqvia_speciality
                        WHERE data_source = 'ORSL'
                    ) I,
                    (
                        select *
                        from ITG_MDS_HCP360_PRODUCT_MAPPING
                        where Brand = 'ORSL'
                    ) M
                WHERE I.product_description = M.IQVIA (+)
            ) iqvia
    ),
    REPORT_BRAND as (
        SELECT DISTINCT country,
            source_system,
            datasource,
            report_brand_reference,
            Speciality_Report,
            activity_date,
            TotalPrescritions_by_speciality
        FROM BASE
        WHERE report_brand_reference not like '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    NULL AS ACTIVITY_TYPE --Check KPI Sheet			
,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.datasource,
    NULL AS brand_category,
    BASE.report_brand_reference,
    NULL AS iqvia_brand,
    BASE.Speciality_Report AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    NULL AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    NULL AS pack_description,
    BASE.product_description,
    BASE.input_brand,
    BASE.noofprescritions,
    BASE.noofprescribers,
    NULL AS mat_noofprescritions,
    NULL AS mat_noofprescribers,
    NULL AS mat_totalprescritions_by_brand,
    NULL AS mat_totalprescribers_by_brand,
    NULL AS mat_totalprescritions_jnj_brand,
    NULL AS TotalPrescritions_by_Brand,
    NULL AS TotalPrescribers_by_Brand,
    NULL AS totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm,
    NULL AS call_type,
    NULL AS email_subject,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.TotalPrescritions_by_speciality
    end as TotalPrescritions_by_speciality,
    BASE.TotalPrescribers_by_speciality,
    REPORT_BRAND.TotalPrescritions_by_speciality as totalprescritions_jnj_speciality,
    NULL AS totalprescritions_by_indication,
    NULL AS totalprescribers_by_indication,
    NULL AS totalprescritions_jnj_indication
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.datasource = REPORT_BRAND.datasource(+)
    AND BASE.Speciality_Report = REPORT_BRAND.Speciality_Report (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
temp6 as 
(
    WITH BASE AS (
        SELECT 'IN'::varchar AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            'DERMA'::varchar AS BRAND,
            iqvia.Report_Brand AS REPORT_BRAND_REFERENCE,
            iqvia.Speciality_Report,
            iqvia.ZONE AS REGION,
            iqvia.ACTIVITY_DATE,
            iqvia.product_description,
            iqvia.no_of_prescriptions AS NoofPrescritions,
            iqvia.no_of_prescribers AS NoofPrescribers,
            iqvia.pack_volume,
            SUM(iqvia.no_of_prescriptions) OVER (
                PARTITION BY Report_Brand,
                iqvia.Speciality_Report,
                activity_date
            ) AS TotalPrescritions_by_speciality,
            SUM(iqvia.no_of_prescribers) OVER (
                PARTITION BY Report_Brand,
                iqvia.Speciality_Report,
                activity_date
            ) AS TotalPrescribers_by_speciality,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT 'DERMA' AS Brand,
                    nvl(
                        M.BRAND,
CASE
                            WHEN POSITION(' ' IN I.Product_description) > 0 THEN SPLIT_PART(I.Product_description, ' ', 1)
                            WHEN POSITION('-' IN I.Product_description) > 0 THEN SPLIT_PART(I.Product_description, '-', 1)
                            ELSE I.Product_description
                        end || '_COMP'
                    ) AS Report_Brand,
                    product_description,
                    Speciality_Report,
                    ZONE,
                    CAST(Year_month AS DATE) AS activity_date,
                    no_of_prescriptions,
                    no_of_prescribers,
                    pack_volume
                FROM (
                        SELECT *,
                            CASE
                                WHEN speciality = 'GP-NON.MBBS' THEN 'GP - Non-MBBS'
                                WHEN speciality = 'GP - MBBS' THEN 'GP - MBBS'
                                WHEN speciality = 'PEDIATRICIAN' THEN 'Pediatrics'
                                WHEN speciality = 'GYNECOLOGIST' THEN 'Gynecology'
                                WHEN speciality = 'CONS. PHYSICIAN' THEN 'Consultation'
                                WHEN speciality NOT IN (
                                    'GP-NON.MBBS',
                                    'GP - MBBS',
                                    'PEDIATRICIAN',
                                    'GYNECOLOGIST',
                                    'CONS. PHYSICIAN'
                                ) THEN 'Others'
                            END AS Speciality_Report
                        FROM itg_hcp360_in_iqvia_speciality
                        WHERE data_source = 'Aveeno_body'
                    ) I,
                    (
                        SELECT CASE
                                WHEN SPLIT_PART(iqvia, ' ', 1) = 'AVEENO' THEN 'AVEENO BODY'
                                ELSE brand
                            END AS brand,
                            iqvia
                        FROM ITG_MDS_HCP360_PRODUCT_MAPPING
                        WHERE brand = 'DERMA'
                    ) M
                WHERE I.product_DESCRIPTION = M.IQVIA (+)
                    AND I.data_source = 'Aveeno_body'
            ) iqvia
    ),
    REPORT_BRAND AS (
        SELECT DISTINCT country,
            source_system,
            BRAND,
            report_brand_reference,
            Speciality_Report,
            activity_date,
            TotalPrescritions_by_speciality
        FROM BASE
        WHERE report_brand_reference NOT LIKE '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    NULL AS ACTIVITY_TYPE --Check KPI Sheet			
,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.brand,
    NULL AS brand_category,
    BASE.report_brand_reference,
    NULL AS iqvia_brand,
    BASE.Speciality_Report AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    NULL AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    NULL AS pack_description,
    BASE.product_description,
    NULL AS input_brand,
    BASE.noofprescritions,
    BASE.noofprescribers,
    NULL AS mat_noofprescritions,
    NULL AS mat_noofprescribers,
    NULL AS mat_totalprescritions_by_brand,
    NULL AS mat_totalprescribers_by_brand,
    NULL AS mat_totalprescritions_jnj_brand,
    NULL AS TotalPrescritions_by_Brand,
    NULL AS TotalPrescribers_by_Brand,
    NULL AS totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm,
    NULL AS call_type,
    NULL AS email_subject,
    CASE
        WHEN BASE.report_brand_reference NOT LIKE '%_COMP' THEN 0
        ELSE BASE.TotalPrescritions_by_speciality
    END AS TotalPrescritions_by_speciality,
    BASE.TotalPrescribers_by_speciality,
    REPORT_BRAND.TotalPrescritions_by_speciality AS totalprescritions_jnj_speciality,
    NULL AS totalprescritions_by_indication,
    NULL AS totalprescribers_by_indication,
    NULL AS totalprescritions_jnj_indication,
    BASE.pack_volume as iqvia_pack_volume
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.brand = REPORT_BRAND.brand(+)
    AND BASE.Speciality_Report = REPORT_BRAND.Speciality_Report (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
temp7 as 
(
    WITH BASE AS (
        SELECT 'IN'::varchar AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            'DERMA'::varchar AS BRAND,
            iqvia.Report_Brand AS REPORT_BRAND_REFERENCE,
            iqvia.Speciality_Report,
            iqvia.ZONE AS REGION,
            iqvia.ACTIVITY_DATE,
            iqvia.product_description,
            iqvia.no_of_prescriptions AS NoofPrescritions,
            iqvia.no_of_prescribers AS NoofPrescribers,
            iqvia.pack_volume,
            SUM(iqvia.no_of_prescriptions) OVER (
                PARTITION BY Report_Brand,
                iqvia.Speciality_Report,
                activity_date
            ) AS TotalPrescritions_by_speciality,
            SUM(iqvia.no_of_prescribers) OVER (
                PARTITION BY Report_Brand,
                iqvia.Speciality_Report,
                activity_date
            ) AS TotalPrescribers_by_speciality,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT 'DERMA' AS Brand,
                    nvl(
                        M.BRAND,
CASE
                            WHEN POSITION(' ' IN I.Product_description) > 0 THEN SPLIT_PART(I.Product_description, ' ', 1)
                            WHEN POSITION('-' IN I.Product_description) > 0 THEN SPLIT_PART(I.Product_description, '-', 1)
                            ELSE I.Product_description
                        end || '_COMP'
                    ) AS Report_Brand,
                    product_description,
                    Speciality_Report,
                    ZONE,
                    CAST(Year_month AS DATE) AS activity_date,
                    no_of_prescriptions,
                    no_of_prescribers,
                    pack_volume
                FROM (
                        SELECT *,
                            CASE
                                WHEN speciality = 'GP-NON.MBBS' THEN 'GP - Non-MBBS'
                                WHEN speciality = 'GP - MBBS' THEN 'GP - MBBS'
                                WHEN speciality = 'PEDIATRICIAN' THEN 'Pediatrics'
                                WHEN speciality = 'GYNECOLOGIST' THEN 'Gynecology'
                                WHEN speciality = 'CONS. PHYSICIAN' THEN 'Consultation'
                                WHEN speciality NOT IN (
                                    'GP-NON.MBBS',
                                    'GP - MBBS',
                                    'PEDIATRICIAN',
                                    'GYNECOLOGIST',
                                    'CONS. PHYSICIAN'
                                ) THEN 'Others'
                            END AS Speciality_Report
                        FROM itg_hcp360_in_iqvia_speciality
                        WHERE data_source = 'Aveeno_baby'
                            and upper(split_part(Product_description, ' ', 1)) != 'JOHNSON'
                    ) I,
                    (
                        select *
                        from ITG_MDS_HCP360_PRODUCT_MAPPING
                        where Brand = 'AVEENO BABY'
                    ) M
                WHERE I.product_DESCRIPTION = M.IQVIA (+)
                    AND I.data_source = 'Aveeno_baby'
            ) iqvia
    ),
    REPORT_BRAND AS (
        SELECT DISTINCT country,
            source_system,
            BRAND,
            report_brand_reference,
            Speciality_Report,
            activity_date,
            TotalPrescritions_by_speciality
        FROM BASE
        WHERE report_brand_reference NOT LIKE '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    NULL AS ACTIVITY_TYPE --Check KPI Sheet			
,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.brand --BASE.datasource
,
    NULL AS brand_category,
    BASE.report_brand_reference,
    NULL AS iqvia_brand,
    BASE.Speciality_Report AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    NULL AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    NULL AS pack_description,
    BASE.product_description,
    BASE.pack_volume,
    NULL AS input_brand,
    BASE.noofprescritions,
    BASE.noofprescribers,
    NULL AS mat_noofprescritions,
    NULL AS mat_noofprescribers,
    NULL AS mat_totalprescritions_by_brand,
    NULL AS mat_totalprescribers_by_brand,
    NULL AS mat_totalprescritions_jnj_brand,
    NULL AS TotalPrescritions_by_Brand,
    NULL AS TotalPrescribers_by_Brand,
    NULL AS totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm,
    NULL AS call_type,
    NULL AS email_subject,
    CASE
        WHEN BASE.report_brand_reference NOT LIKE '%_COMP' THEN 0
        ELSE BASE.TotalPrescritions_by_speciality
    END AS TotalPrescritions_by_speciality,
    BASE.TotalPrescribers_by_speciality,
    REPORT_BRAND.TotalPrescritions_by_speciality AS totalprescritions_jnj_speciality,
    NULL AS totalprescritions_by_indication,
    NULL AS totalprescribers_by_indication,
    NULL AS totalprescritions_jnj_indication
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.brand = REPORT_BRAND.brand(+)
    AND BASE.Speciality_Report = REPORT_BRAND.Speciality_Report (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
temp8 as 
(
    WITH BASE AS (
        SELECT 'IN'::varchar AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            'JBABY'::varchar AS BRAND,
            iqvia.Report_Brand AS REPORT_BRAND_REFERENCE,
            iqvia.Speciality_Report,
            iqvia.ZONE AS REGION,
            iqvia.ACTIVITY_DATE,
            iqvia.product_description,
            iqvia.no_of_prescriptions AS NoofPrescritions,
            iqvia.no_of_prescribers AS NoofPrescribers,
            iqvia.pack_volume,
            SUM(iqvia.no_of_prescriptions) OVER (
                PARTITION BY Report_Brand,
                iqvia.Speciality_Report,
                activity_date
            ) AS TotalPrescritions_by_speciality,
            SUM(iqvia.no_of_prescribers) OVER (
                PARTITION BY Report_Brand,
                iqvia.Speciality_Report,
                activity_date
            ) AS TotalPrescribers_by_speciality,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT 'JBABY' AS Brand,
                    nvl(
                        M.BRAND,
CASE
                            WHEN POSITION(' ' IN I.Product_description) > 0 THEN SPLIT_PART(I.Product_description, ' ', 1)
                            WHEN POSITION('-' IN I.Product_description) > 0 THEN SPLIT_PART(I.Product_description, '-', 1)
                            ELSE I.Product_description
                        end || '_COMP'
                    ) AS Report_Brand,
                    product_description,
                    Speciality_Report,
                    ZONE,
                    CAST(Year_month AS DATE) AS activity_date,
                    no_of_prescriptions,
                    no_of_prescribers,
                    pack_volume
                FROM (
                        SELECT *,
                            CASE
                                WHEN speciality = 'GP-NON.MBBS' THEN 'GP - Non-MBBS'
                                WHEN speciality = 'GP - MBBS' THEN 'GP - MBBS'
                                WHEN speciality = 'PEDIATRICIAN' THEN 'Pediatrics'
                                WHEN speciality = 'GYNECOLOGIST' THEN 'Gynecology'
                                WHEN speciality = 'CONS. PHYSICIAN' THEN 'Consultation'
                                WHEN speciality NOT IN (
                                    'GP-NON.MBBS',
                                    'GP - MBBS',
                                    'PEDIATRICIAN',
                                    'GYNECOLOGIST',
                                    'CONS. PHYSICIAN'
                                ) THEN 'Others'
                            END AS Speciality_Report
                        FROM itg_hcp360_in_iqvia_speciality
                        WHERE data_source = 'Aveeno_baby'
                            and upper(split_part(Product_description, ' ', 1)) != 'AVEENO'
                            and (
                                upper(brand) like '%BABY%'
                                OR upper(Product_description) like '%BABY%'
                            )
                    ) I,
                    (
                        select *
                        from ITG_MDS_HCP360_PRODUCT_MAPPING
                        where Brand = 'JBABY'
                    ) M
                WHERE I.brand = M.IQVIA (+)
                    AND I.data_source = 'Aveeno_baby'
            ) iqvia
    ),
    REPORT_BRAND AS (
        SELECT DISTINCT country,
            source_system,
            BRAND,
            report_brand_reference,
            Speciality_Report,
            activity_date,
            TotalPrescritions_by_speciality
        FROM BASE
        WHERE report_brand_reference NOT LIKE '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    NULL AS ACTIVITY_TYPE --Check KPI Sheet			
,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.brand --BASE.datasource
,
    NULL AS brand_category,
    BASE.report_brand_reference,
    NULL AS iqvia_brand,
    BASE.Speciality_Report AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    NULL AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    NULL AS pack_description,
    BASE.product_description,
    BASE.pack_volume,
    NULL AS input_brand,
    BASE.noofprescritions,
    BASE.noofprescribers,
    NULL AS mat_noofprescritions,
    NULL AS mat_noofprescribers,
    NULL AS mat_totalprescritions_by_brand,
    NULL AS mat_totalprescribers_by_brand,
    NULL AS mat_totalprescritions_jnj_brand,
    NULL AS TotalPrescritions_by_Brand,
    NULL AS TotalPrescribers_by_Brand,
    NULL AS totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm,
    NULL AS call_type,
    NULL AS email_subject,
    CASE
        WHEN BASE.report_brand_reference NOT LIKE '%_COMP' THEN 0
        ELSE BASE.TotalPrescritions_by_speciality
    END AS TotalPrescritions_by_speciality,
    BASE.TotalPrescribers_by_speciality,
    REPORT_BRAND.TotalPrescritions_by_speciality AS totalprescritions_jnj_speciality,
    NULL AS totalprescritions_by_indication,
    NULL AS totalprescribers_by_indication,
    NULL AS totalprescritions_jnj_indication
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.brand = REPORT_BRAND.brand(+)
    AND BASE.Speciality_Report = REPORT_BRAND.Speciality_Report (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
temp9 as 
(
    with BASE as (
        SELECT 'IN'::varchar AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            iqvia.data_source AS datasource,
            iqvia.Report_Brand AS REPORT_BRAND_REFERENCE,
            iqvia.diagnosis,
            iqvia.ZONE AS REGION,
            iqvia.ACTIVITY_DATE,
            iqvia.product_description,
            iqvia.input_brand,
            iqvia.no_of_prescriptions AS NoofPrescritions,
            iqvia.no_of_prescribers AS NoofPrescribers,
            SUM(iqvia.no_of_prescriptions) OVER (
                PARTITION BY Report_Brand,
                iqvia.diagnosis,
                activity_date
            ) AS TotalPrescritions_by_indication,
            SUM(iqvia.no_of_prescribers) OVER (
                PARTITION BY Report_Brand,
                iqvia.diagnosis,
                activity_date
            ) AS TotalPrescribers_by_indication,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT data_source,
                    ZONE,
                    product_description,
                    i.brand as input_brand,
                    cast(I.Year_month as Date) AS ACTIVITY_DATE,
                    no_of_prescriptions,
                    no_of_prescribers,
                    CASE
                        WHEN upper(Product_Description) = 'ORSL TOTAL' THEN 'ORSL'
                        ELSE i.brand || '_COMP'
                    END AS Report_Brand,
                    i.diagnosis
                FROM itg_hcp360_in_iqvia_indication I
            ) iqvia
    ),
    REPORT_BRAND as (
        SELECT DISTINCT country,
            source_system,
            datasource,
            report_brand_reference,
            diagnosis,
            activity_date,
            TotalPrescritions_by_indication
        FROM BASE
        WHERE report_brand_reference not like '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    NULL AS ACTIVITY_TYPE --Check KPI Sheet			
,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.datasource,
    NULL AS brand_category,
    BASE.report_brand_reference,
    NULL AS iqvia_brand,
    NULL AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    NULL AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    base.diagnosis AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    NULL AS pack_description,
    BASE.product_description,
    BASE.input_brand,
    BASE.noofprescritions,
    BASE.noofprescribers,
    NULL AS mat_noofprescritions,
    NULL AS mat_noofprescribers,
    NULL AS mat_totalprescritions_by_brand,
    NULL AS mat_totalprescribers_by_brand,
    NULL AS mat_totalprescritions_jnj_brand,
    NULL AS TotalPrescritions_by_Brand,
    NULL AS TotalPrescribers_by_Brand,
    NULL AS totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm,
    NULL AS call_type,
    NULL AS email_subject,
    NULL AS totalprescritions_by_speciality,
    NULL AS totalprescribers_by_speciality,
    NULL AS totalprescritions_jnj_speciality,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.TotalPrescritions_by_indication
    end as TotalPrescritions_by_indication,
    BASE.TotalPrescribers_by_indication,
    REPORT_BRAND.TotalPrescritions_by_indication as totalprescritions_jnj_indication
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.datasource = REPORT_BRAND.datasource(+)
    AND BASE.diagnosis = REPORT_BRAND.diagnosis (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
temp10 as 
(
    with BASE as (
        SELECT iqvia.country AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            iqvia.data_source AS datasource,
            iqvia.brand_category AS BRAND_CATEGORY,
            iqvia.Report_Brand As REPORT_BRAND_REFERENCE --,case when Report_Brand like'%COMP' then NULL else 'ORSL Total' end  as IQVIA_BRAND				
,
            'ORSL Total'::varchar as IQVIA_BRAND,
            iqvia.region AS REGION,
            iqvia.zone AS zone,
            iqvia.ACTIVITY_DATE,
            iqvia.pack_description,
            iqvia.product_description,
            iqvia.input_brand,
            iqvia.total_units AS sales_unit,
            iqvia.value AS sales_value,
            sum(iqvia.total_units) over (
                partition by Report_Brand,
                iqvia.brand_category,
                activity_date
            ) as Totalsales_unit_by_Brand,
            sum(iqvia.value) over (
                partition by Report_Brand,
                iqvia.brand_category,
                activity_date
            ) as Totalsales_value_by_Brand,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT nvl(M.BRAND, I.BRAND || '_COMP') AS Report_Brand,
                    I.brand as input_brand,
                    I.country,
                    I.state as zone,
                    I.region,
                    I.product_description,
                    I.pack_description,
                    I.brand_category,
                    CAST(I.Year_month AS DATE) AS ACTIVITY_DATE,
                    I.total_units,
                    I.value,
                    I.data_source
                FROM edw_hcp360_in_iqvia_sales I
                    LEFT OUTER JOIN (
                        SELECT iqvia,
                            brand
                        FROM ITG_MDS_HCP360_PRODUCT_MAPPING
                        WHERE Brand = 'ORSL'
                    ) M ON i.PACK_DESCRIPTION = m.iqvia
                WHERE I.data_source = 'ORSL'
            ) iqvia
    ),
    REPORT_BRAND as (
        SELECT DISTINCT country,
            source_system,
            datasource,
            brand_category,
            report_brand_reference,
            iqvia_brand,
            activity_date,
            Totalsales_value_by_Brand,
            Totalsales_unit_by_Brand
        FROM BASE
        WHERE report_brand_reference not like '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    'IQVIA_SALES' AS ACTIVITY_TYPE,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.datasource,
    BASE.brand_category,
    BASE.report_brand_reference,
    BASE.iqvia_brand,
    NULL AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    BASE.zone AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    BASE.pack_description,
    BASE.product_description,
    BASE.input_brand,
    null as noofprescritions,
    NULL AS noofprescribers,
    NULL AS mat_noofprescritions,
    NULL AS mat_noofprescribers,
    NULL AS mat_totalprescritions_by_brand,
    NULL AS mat_totalprescribers_by_brand,
    NULL AS mat_totalprescritions_jnj_brand,
    NULL as TotalPrescritions_by_Brand,
    NULL AS TotalPrescribers_by_Brand,
    NULL as totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm,
    NULL AS call_type,
    NULL AS email_subject,
    NULL AS TotalPrescritions_by_speciality,
    NULL AS TotalPrescribers_by_speciality,
    NULL AS totalprescritions_jnj_speciality,
    NULL AS totalprescritions_by_indication,
    NULL AS totalprescribers_by_indication,
    NULL AS totalprescritions_jnj_indication,
    BASE.sales_unit AS sales_unit,
    BASE.sales_value AS sales_value,
case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.Totalsales_unit_by_Brand
    end AS Totalsales_unit_by_Brand,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.Totalsales_value_by_Brand
    end AS Totalsales_value_by_Brand,
    REPORT_BRAND.Totalsales_unit_by_Brand AS Totalsales_unit_by_jnj_Brand,
    REPORT_BRAND.Totalsales_value_by_Brand AS Totalsales_value_by_jnj_Brand
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.datasource = REPORT_BRAND.datasource(+)
    AND BASE.brand_category = REPORT_BRAND.brand_category (+)
    /*Specifically for ORSL*/
    AND BASE.iqvia_brand = REPORT_BRAND.iqvia_brand (+) --AND BASE.report_brand_reference = REPORT_BRAND. report_brand_reference (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
temp11 as 
(
    with BASE as (
        SELECT iqvia.country AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            'DERMA'::varchar AS BRAND,
            iqvia.data_source AS datasource,
            iqvia.brand_category AS BRAND_CATEGORY,
            iqvia.Report_Brand As REPORT_BRAND_REFERENCE --,case when Report_Brand like'%COMP' then NULL else 'ORSL Total' end  as IQVIA_BRAND				
,
            'AVEENO BODY'::varchar as IQVIA_BRAND,
            iqvia.region AS REGION,
            iqvia.zone AS zone,
            iqvia.ACTIVITY_DATE,
            iqvia.pack_description,
            iqvia.product_description,
            iqvia.input_brand,
            iqvia.total_units AS sales_unit,
            iqvia.value AS sales_value,
            sum(iqvia.total_units) over (
                partition by Report_Brand,
                iqvia.brand_category,
                activity_date
            ) as Totalsales_unit_by_Brand,
            sum(iqvia.value) over (
                partition by Report_Brand,
                iqvia.brand_category,
                activity_date
            ) as Totalsales_value_by_Brand,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT nvl(
                        M.BRAND,
case
                            when position(' ' in I.Product_description) > 0 then split_part(I.Product_description, ' ', 1)
                            when position('-' in I.Product_description) > 0 then split_part(I.Product_description, '-', 1)
                            else I.Product_description
                        end || '_COMP'
                    ) as Report_Brand,
                    'DERMA' as input_brand,
                    I.country,
                    I.state as zone,
                    I.region,
                    I.product_description,
                    I.pack_description,
                    I.brand_category,
                    CAST(I.Year_month AS DATE) AS ACTIVITY_DATE,
                    I.total_units,
                    I.value,
                    I.data_source
                FROM edw_hcp360_in_iqvia_sales I
                    LEFT OUTER JOIN (
                        SELECT iqvia,
                            case
                                when split_part(iqvia, ' ', 1) = 'AVEENO' then 'AVEENO BODY'
                                else brand
                            end as brand
                        FROM ITG_MDS_HCP360_PRODUCT_MAPPING
                        WHERE Brand = 'DERMA'
                    ) M ON i.PACK_DESCRIPTION = m.iqvia
                WHERE I.data_source = 'Aveeno_body'
            ) iqvia
    ),
    REPORT_BRAND as (
        SELECT DISTINCT country,
            source_system,
            datasource,
            brand_category,
            report_brand_reference,
            iqvia_brand,
            activity_date,
            Totalsales_value_by_Brand,
            Totalsales_unit_by_Brand
        FROM BASE
        WHERE report_brand_reference not like '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    'IQVIA_SALES' AS ACTIVITY_TYPE,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.brand,
    BASE.brand_category,
    BASE.report_brand_reference,
    BASE.iqvia_brand,
    NULL AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    BASE.zone AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    BASE.pack_description,
    BASE.product_description --,	BASE.input_brand
,
    null as noofprescritions,
    NULL AS noofprescribers,
    NULL AS mat_noofprescritions,
    NULL AS mat_noofprescribers,
    NULL AS mat_totalprescritions_by_brand,
    NULL AS mat_totalprescribers_by_brand,
    NULL AS mat_totalprescritions_jnj_brand,
    NULL as TotalPrescritions_by_Brand,
    NULL AS TotalPrescribers_by_Brand,
    NULL as totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm,
    NULL AS call_type,
    NULL AS email_subject,
    NULL AS TotalPrescritions_by_speciality,
    NULL AS TotalPrescribers_by_speciality,
    NULL AS totalprescritions_jnj_speciality,
    NULL AS totalprescritions_by_indication,
    NULL AS totalprescribers_by_indication,
    NULL AS totalprescritions_jnj_indication,
    BASE.sales_unit AS sales_unit,
    BASE.sales_value AS sales_value,
case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.Totalsales_unit_by_Brand
    end AS Totalsales_unit_by_Brand,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.Totalsales_value_by_Brand
    end AS Totalsales_value_by_Brand,
    REPORT_BRAND.Totalsales_unit_by_Brand AS Totalsales_unit_by_jnj_Brand,
    REPORT_BRAND.Totalsales_value_by_Brand AS Totalsales_value_by_jnj_Brand
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.datasource = REPORT_BRAND.datasource(+)
    AND BASE.iqvia_brand = REPORT_BRAND.iqvia_brand (+) --AND BASE.report_brand_reference = REPORT_BRAND. report_brand_reference (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
temp12 as 
(
    with BASE as (
        SELECT iqvia.country AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            'DERMA'::varchar AS BRAND,
            iqvia.data_source AS datasource,
            iqvia.brand_category AS BRAND_CATEGORY,
            iqvia.Report_Brand As REPORT_BRAND_REFERENCE --,case when Report_Brand like'%COMP' then NULL else 'ORSL Total' end  as IQVIA_BRAND				
,
            'AVEENO BABY'::varchar as IQVIA_BRAND,
            iqvia.region AS REGION,
            iqvia.zone AS zone,
            iqvia.ACTIVITY_DATE,
            iqvia.pack_description,
            iqvia.product_description --,iqvia.brand  
,
            iqvia.total_units AS sales_unit,
            iqvia.value AS sales_value,
            sum(iqvia.total_units) over (
                partition by Report_Brand,
                iqvia.brand_category,
                activity_date
            ) as Totalsales_unit_by_Brand,
            sum(iqvia.value) over (
                partition by Report_Brand,
                iqvia.brand_category,
                activity_date
            ) as Totalsales_value_by_Brand,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT nvl(
                        M.BRAND,
case
                            when position(' ' in I.Product_description) > 0 then split_part(I.Product_description, ' ', 1)
                            when position('-' in I.Product_description) > 0 then split_part(I.Product_description, '-', 1)
                            else I.Product_description
                        end || '_COMP'
                    ) as Report_Brand,
                    'DERMA' as brand,
                    I.country,
                    I.state as zone,
                    I.region,
                    I.product_description,
                    I.pack_description,
                    I.brand_category,
                    CAST(I.Year_month AS DATE) AS ACTIVITY_DATE,
                    I.total_units,
                    I.value,
                    I.data_source
                FROM edw_hcp360_in_iqvia_sales I
                    LEFT OUTER JOIN (
                        SELECT iqvia,
                            case
                                when split_part(iqvia, ' ', 1) = 'AVEENO' then 'AVEENO BABY'
                                else brand
                            end as brand
                        FROM ITG_MDS_HCP360_PRODUCT_MAPPING
                        WHERE Brand = 'AVEENO BABY'
                    ) M ON i.PACK_DESCRIPTION = m.iqvia
                WHERE I.data_source = 'Aveeno_baby'
                    and upper (split_part(I.Product_description, ' ', 1)) != 'JOHNSON'
            ) iqvia
    ),
    REPORT_BRAND as (
        SELECT DISTINCT country,
            source_system,
            datasource,
            brand_category,
            report_brand_reference,
            iqvia_brand,
            activity_date,
            Totalsales_value_by_Brand,
            Totalsales_unit_by_Brand
        FROM BASE
        WHERE report_brand_reference not like '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    'IQVIA_SALES' AS ACTIVITY_TYPE,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.brand,
    BASE.brand_category,
    BASE.report_brand_reference,
    BASE.iqvia_brand,
    NULL AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    BASE.zone AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    BASE.pack_description,
    BASE.product_description --,	BASE.input_brand
,
    null as noofprescritions,
    NULL AS noofprescribers,
    NULL AS mat_noofprescritions,
    NULL AS mat_noofprescribers,
    NULL AS mat_totalprescritions_by_brand,
    NULL AS mat_totalprescribers_by_brand,
    NULL AS mat_totalprescritions_jnj_brand,
    NULL as TotalPrescritions_by_Brand,
    NULL AS TotalPrescribers_by_Brand,
    NULL as totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm,
    NULL AS call_type,
    NULL AS email_subject,
    NULL AS TotalPrescritions_by_speciality,
    NULL AS TotalPrescribers_by_speciality,
    NULL AS totalprescritions_jnj_speciality,
    NULL AS totalprescritions_by_indication,
    NULL AS totalprescribers_by_indication,
    NULL AS totalprescritions_jnj_indication,
    BASE.sales_unit AS sales_unit,
    BASE.sales_value AS sales_value,
case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.Totalsales_unit_by_Brand
    end AS Totalsales_unit_by_Brand,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.Totalsales_value_by_Brand
    end AS Totalsales_value_by_Brand,
    REPORT_BRAND.Totalsales_unit_by_Brand AS Totalsales_unit_by_jnj_Brand,
    REPORT_BRAND.Totalsales_value_by_Brand AS Totalsales_value_by_jnj_Brand
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.datasource = REPORT_BRAND.datasource(+)
    AND BASE.iqvia_brand = REPORT_BRAND.iqvia_brand (+) --AND BASE.report_brand_reference = REPORT_BRAND. report_brand_reference (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
temp13 as 
(
    with BASE as (
        SELECT iqvia.country AS COUNTRY,
            'IQVIA'::varchar AS SOURCE_SYSTEM,
            'JBABY'::varchar AS BRAND,
            iqvia.data_source AS datasource,
            iqvia.brand_category AS BRAND_CATEGORY,
            iqvia.Report_Brand As REPORT_BRAND_REFERENCE --,case when Report_Brand like'%COMP' then NULL else 'ORSL Total' end  as IQVIA_BRAND				
,
            'JBABY Total'::varchar as IQVIA_BRAND,
            iqvia.region AS REGION,
            iqvia.zone AS zone,
            iqvia.ACTIVITY_DATE,
            iqvia.pack_description,
            iqvia.product_description --,iqvia.brand  
,
            iqvia.total_units AS sales_unit,
            iqvia.value AS sales_value,
            sum(iqvia.total_units) over (
                partition by Report_Brand,
                iqvia.brand_category,
                activity_date
            ) as Totalsales_unit_by_Brand,
            sum(iqvia.value) over (
                partition by Report_Brand,
                iqvia.brand_category,
                activity_date
            ) as Totalsales_value_by_Brand,
            convert_timezone('UTC',current_timestamp()) AS CRT_DTTM,
            convert_timezone('UTC',current_timestamp()) AS UPDT_DTTM
        FROM (
                SELECT nvl(
                        M.BRAND,
case
                            when position(' ' in I.Product_description) > 0 then split_part(I.Product_description, ' ', 1)
                            when position('-' in I.Product_description) > 0 then split_part(I.Product_description, '-', 1)
                            else I.Product_description
                        end || '_COMP'
                    ) as Report_Brand,
                    'JBABY' as brand,
                    I.country,
                    I.state as zone,
                    I.region,
                    I.product_description,
                    I.pack_description,
                    I.brand_category,
                    CAST(I.Year_month AS DATE) AS ACTIVITY_DATE,
                    I.total_units,
                    I.value,
                    I.data_source
                FROM edw_hcp360_in_iqvia_sales I
                    LEFT OUTER JOIN (
                        SELECT iqvia,
                            brand
                        FROM ITG_MDS_HCP360_PRODUCT_MAPPING
                        WHERE Brand = 'JBABY'
                    ) M ON i.PACK_DESCRIPTION = m.iqvia
                WHERE I.data_source = 'Aveeno_baby'
                    and upper (split_part(I.Product_description, ' ', 1)) != 'AVEENO'
                    and upper(pack_description) like '%BABY%'
            ) iqvia
    ),
    REPORT_BRAND as (
        SELECT DISTINCT country,
            source_system,
            datasource,
            brand_category,
            report_brand_reference,
            iqvia_brand,
            activity_date,
            Totalsales_value_by_Brand,
            Totalsales_unit_by_Brand
        FROM BASE
        WHERE report_brand_reference not like '%COMP'
    )
SELECT BASE.country,
    BASE.source_system,
    NULL AS CHANNEL --Check KPI Sheet 				
,
    'IQVIA_SALES' AS ACTIVITY_TYPE,
    NULL AS HCP_ID,
    NULL AS HCP_MASTER_ID,
    NULL AS EMPLOYEE_ID,
    BASE.brand,
    BASE.brand_category,
    BASE.report_brand_reference,
    BASE.iqvia_brand,
    NULL AS SPECIALITY,
    NULL AS CORE_NONCORE,
    NULL AS CLASSIFICATION,
    NULL AS TERRITORY,
    BASE.region,
    BASE.zone AS ZONE,
    NULL AS HCP_CREATED_DATE,
    BASE.activity_date,
    NULL AS CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NULL AS NO_OF_PRESCRIPTION_UNITS,
    NULL AS FIRST_PRESCRIPTION_DATE,
    NULL AS PLANNED_CALL_CNT,
    NULL AS CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    BASE.pack_description,
    BASE.product_description --,	BASE.input_brand
,
    null as noofprescritions,
    NULL AS noofprescribers,
    NULL AS mat_noofprescritions,
    NULL AS mat_noofprescribers,
    NULL AS mat_totalprescritions_by_brand,
    NULL AS mat_totalprescribers_by_brand,
    NULL AS mat_totalprescritions_jnj_brand,
    NULL as TotalPrescritions_by_Brand,
    NULL AS TotalPrescribers_by_Brand,
    NULL as totalprescritions_jnj_brand,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    BASE.crt_dttm,
    BASE.updt_dttm,
    NULL AS call_type,
    NULL AS email_subject,
    NULL AS TotalPrescritions_by_speciality,
    NULL AS TotalPrescribers_by_speciality,
    NULL AS totalprescritions_jnj_speciality,
    NULL AS totalprescritions_by_indication,
    NULL AS totalprescribers_by_indication,
    NULL AS totalprescritions_jnj_indication,
    BASE.sales_unit AS sales_unit,
    BASE.sales_value AS sales_value,
case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.Totalsales_unit_by_Brand
    end AS Totalsales_unit_by_Brand,
    case
        when BASE.report_brand_reference not like '%_COMP' then 0
        else BASE.Totalsales_value_by_Brand
    end AS Totalsales_value_by_Brand,
    REPORT_BRAND.Totalsales_unit_by_Brand AS Totalsales_unit_by_jnj_Brand,
    REPORT_BRAND.Totalsales_value_by_Brand AS Totalsales_value_by_jnj_Brand
FROM BASE,
    REPORT_BRAND
WHERE BASE.country = REPORT_BRAND.country (+)
    AND BASE.source_system = REPORT_BRAND.source_System (+)
    AND BASE.datasource = REPORT_BRAND.datasource(+) -- AND  BASE.brand_category = REPORT_BRAND.brand_category   (+)
    AND BASE.iqvia_brand = REPORT_BRAND.iqvia_brand (+) -- AND BASE.report_brand_reference = REPORT_BRAND. report_brand_reference (+)
    AND BASE.activity_date = REPORT_BRAND.activity_date (+)
),
trans1 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    datasource as brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    input_brand as iqvia_input_brand,
    null as iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    null as call_type,
    null as email_subject,
    null as totalprescritions_by_speciality,
    null as totalprescribers_by_speciality,
    null as totalprescritions_jnj_speciality,
    null as totalprescritions_by_indication,
    null as totalprescribers_by_indication,
    null as totalprescritions_jnj_indication,
    null as sales_unit,
    null as sales_value,
    null as totalsales_unit_by_brand,
    null as totalsales_value_by_brand,
    null as totalsales_unit_by_jnj_brand,
    null as totalsales_value_by_jnj_brand
from temp1
),
trans2 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    null as iqvia_input_brand,
    pack_volume as iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    null as call_type,
    null as email_subject,
    null as totalprescritions_by_speciality,
    null as totalprescribers_by_speciality,
    null as totalprescritions_jnj_speciality,
    null as totalprescritions_by_indication,
    null as totalprescribers_by_indication,
    null as totalprescritions_jnj_indication,
    null as sales_unit,
    null as sales_value,
    null as totalsales_unit_by_brand,
    null as totalsales_value_by_brand,
    null as totalsales_unit_by_jnj_brand,
    null as totalsales_value_by_jnj_brand
    from temp2
),
trans3 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    null as iqvia_input_brand,
    pack_volume as iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    null as call_type,
    null as email_subject,
    null as totalprescritions_by_speciality,
    null as totalprescribers_by_speciality,
    null as totalprescritions_jnj_speciality,
    null as totalprescritions_by_indication,
    null as totalprescribers_by_indication,
    null as totalprescritions_jnj_indication,
    null as sales_unit,
    null as sales_value,
    null as totalsales_unit_by_brand,
    null as totalsales_value_by_brand,
    null as totalsales_unit_by_jnj_brand,
    null as totalsales_value_by_jnj_brand
    from temp3
),
trans4 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    null as iqvia_input_brand,
    pack_volume as iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    null as call_type,
    null as email_subject,
    null as totalprescritions_by_speciality,
    null as totalprescribers_by_speciality,
    null as totalprescritions_jnj_speciality,
    null as totalprescritions_by_indication,
    null as totalprescribers_by_indication,
    null as totalprescritions_jnj_indication,
    null as sales_unit,
    null as sales_value,
    null as totalsales_unit_by_brand,
    null as totalsales_value_by_brand,
    null as totalsales_unit_by_jnj_brand,
    null as totalsales_value_by_jnj_brand
    from temp4
),
trans5 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    datasource as brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    input_brand as iqvia_input_brand,
    null as iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    call_type,
    email_subject,
    totalprescritions_by_speciality,
    totalprescribers_by_speciality,
    totalprescritions_jnj_speciality,
    totalprescritions_by_indication,
    totalprescribers_by_indication,
    totalprescritions_jnj_indication,
    null as sales_unit,
    null as sales_value,
    null as totalsales_unit_by_brand,
    null as totalsales_value_by_brand,
    null as totalsales_unit_by_jnj_brand,
    null as totalsales_value_by_jnj_brand
from temp5
),
trans6 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    input_brand as iqvia_input_brand,
    iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    call_type,
    email_subject,
    totalprescritions_by_speciality,
    totalprescribers_by_speciality,
    totalprescritions_jnj_speciality,
    totalprescritions_by_indication,
    totalprescribers_by_indication,
    totalprescritions_jnj_indication,
    null as sales_unit,
    null as sales_value,
    null as totalsales_unit_by_brand,
    null as totalsales_value_by_brand,
    null as totalsales_unit_by_jnj_brand,
    null as totalsales_value_by_jnj_brand
    from temp6
),
trans7 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    input_brand as iqvia_input_brand,
    pack_volume as iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    call_type,
    email_subject,
    totalprescritions_by_speciality,
    totalprescribers_by_speciality,
    totalprescritions_jnj_speciality,
    totalprescritions_by_indication,
    totalprescribers_by_indication,
    totalprescritions_jnj_indication,
    null as sales_unit,
    null as sales_value,
    null as totalsales_unit_by_brand,
    null as totalsales_value_by_brand,
    null as totalsales_unit_by_jnj_brand,
    null as totalsales_value_by_jnj_brand
    from temp7
),
trans8 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    input_brand as iqvia_input_brand,
    pack_volume as iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    call_type,
    email_subject,
    totalprescritions_by_speciality,
    totalprescribers_by_speciality,
    totalprescritions_jnj_speciality,
    totalprescritions_by_indication,
    totalprescribers_by_indication,
    totalprescritions_jnj_indication,
    null as sales_unit,
    null as sales_value,
    null as totalsales_unit_by_brand,
    null as totalsales_value_by_brand,
    null as totalsales_unit_by_jnj_brand,
    null as totalsales_value_by_jnj_brand
    from temp8
),
trans9 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    datasource as brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    input_brand as iqvia_input_brand,
    null as iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    call_type,
    email_subject,
    totalprescritions_by_speciality,
    totalprescribers_by_speciality,
    totalprescritions_jnj_speciality,
    totalprescritions_by_indication,
    totalprescribers_by_indication,
    totalprescritions_jnj_indication,
    null as sales_unit,
    null as sales_value,
    null as totalsales_unit_by_brand,
    null as totalsales_value_by_brand,
    null as totalsales_unit_by_jnj_brand,
    null as totalsales_value_by_jnj_brand
    from temp9
),
trans10 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    datasource as brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    input_brand as iqvia_input_brand,
    null as iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    call_type,
    email_subject,
    totalprescritions_by_speciality,
    totalprescribers_by_speciality,
    totalprescritions_jnj_speciality,
    totalprescritions_by_indication,
    totalprescribers_by_indication,
    totalprescritions_jnj_indication,
    sales_unit,
    sales_value,
    totalsales_unit_by_brand,
    totalsales_value_by_brand,
    totalsales_unit_by_jnj_brand,
    totalsales_value_by_jnj_brand
    from temp10
),
trans11 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    null as iqvia_input_brand,
    null as iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    call_type,
    email_subject,
    totalprescritions_by_speciality,
    totalprescribers_by_speciality,
    totalprescritions_jnj_speciality,
    totalprescritions_by_indication,
    totalprescribers_by_indication,
    totalprescritions_jnj_indication,
    sales_unit,
    sales_value,
    totalsales_unit_by_brand,
    totalsales_value_by_brand,
    totalsales_unit_by_jnj_brand,
    totalsales_value_by_jnj_brand
    from temp11
),
trans12 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    null as iqvia_input_brand,
    null as iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    call_type,
    email_subject,
    totalprescritions_by_speciality,
    totalprescribers_by_speciality,
    totalprescritions_jnj_speciality,
    totalprescritions_by_indication,
    totalprescribers_by_indication,
    totalprescritions_jnj_indication,
    sales_unit,
    sales_value,
    totalsales_unit_by_brand,
    totalsales_value_by_brand,
    totalsales_unit_by_jnj_brand,
    totalsales_value_by_jnj_brand
    from temp12
),
trans13 as 
(
    select
    country,
    source_system,
    channel,
    activity_type,
    hcp_id,
    hcp_master_id,
    employee_id,
    brand,
    brand_category,
    report_brand_reference,
    iqvia_brand,
    speciality,
    core_noncore,
    classification,
    territory,
    region,
    zone,
    hcp_created_date,
    activity_date,
    call_source_id,
    product_indication_name,
    no_of_prescription_units,
    first_prescription_date,
    planned_call_cnt,
    call_duration,
    diagnosis,
    prescription_id,
    pack_description as iqvia_pack_description,
    product_description as iqvia_product_description,
    null as iqvia_input_brand,
    null as iqvia_pack_volume,
    noofprescritions,
    noofprescribers,
    mat_noofprescritions,
    mat_noofprescribers,
    mat_totalprescritions_by_brand,
    mat_totalprescribers_by_brand,
    mat_totalprescritions_jnj_brand,
    totalprescritions_by_brand,
    totalprescribers_by_brand,
    totalprescritions_jnj_brand,
    email_name,
    is_unique,
    email_delivered_flag,
    email_activity_type,
    crt_dttm,
    updt_dttm,
    call_type,
    email_subject,
    totalprescritions_by_speciality,
    totalprescribers_by_speciality,
    totalprescritions_jnj_speciality,
    totalprescritions_by_indication,
    totalprescribers_by_indication,
    totalprescritions_jnj_indication,
    sales_unit,
    sales_value,
    totalsales_unit_by_brand,
    totalsales_value_by_brand,
    totalsales_unit_by_jnj_brand,
    totalsales_value_by_jnj_brand
    from temp13
),

final as 
(
    select * from trans1
    union all
    select * from trans2
    union all
    select * from trans3
    union all
    select * from trans4
    union all
    select * from trans5
    union all
    select * from trans6
    union all
    select * from trans7
    union all
    select * from trans8
    union all
    select * from trans9
    union all
    select * from trans10
    union all
    select * from trans11
    union all
    select * from trans12
    union all
    select * from trans13
)
select * from final