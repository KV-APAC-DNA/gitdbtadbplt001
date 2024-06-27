{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE UPPER(TRIM(Visit_ID)) ||UPPER(TRIM(LEFT(Visit_DateTime,10))) ||UPPER(TRIM(Region)) ||UPPER(TRIM(JnJRKAM)) ||UPPER(TRIM(JnJZM_Code)) ||UPPER(TRIM(JNJ_ABI_Code)) ||UPPER(TRIM(JnJSupervisor_Code)) ||UPPER(TRIM(ISP_Code)) ||UPPER(TRIM(ISP_Name)) 
        ||UPPER(TRIM(MONTH)) ||UPPER(TRIM(YEAR)) ||UPPER(TRIM(Format)) ||UPPER(TRIM(Chain_Code)) ||UPPER(TRIM(Chain)) ||UPPER(TRIM(Store_Code)) ||UPPER(TRIM(Store_Name)) ||UPPER(TRIM(Asset)) ||UPPER(TRIM(Product_Category)) ||UPPER(TRIM(Product_Brand)) ||UPPER(TRIM(POSM_Brand)) ||UPPER(TRIM(Start_Date)) ||UPPER(TRIM(End_Date)) ||UPPER(TRIM(Audit_Status)) ||UPPER(TRIM(Is_Available)) 
        ||UPPER(TRIM(Availability_Points)) ||UPPER(TRIM(Visibility_Type)) ||UPPER(TRIM(Visibility_Condition)) ||UPPER(TRIM(Is_Planogram_Availbale)) ||UPPER(TRIM(Select_Brand)) ||UPPER(TRIM(Is_Correct_Brand_Displayed)) ||UPPER(TRIM(BrandAvailability_Points))||UPPER(TRIM(Stock_Status)) ||UPPER(TRIM(Stock_Points)) ||UPPER(TRIM(Is_Near_Category)) ||UPPER(TRIM(NearCategory_Points)) ||UPPER(TRIM(Audit_Score)) ||UPPER(TRIM(Paid_Visibility_Score)) ||UPPER(TRIM(Reason)) ||UPPER(TRIM(Priority_Store)) 
        IN 
        (SELECT DISTINCT UPPER(TRIM(Visit_ID)) ||UPPER(TRIM(Visit_DateTime)) ||UPPER(TRIM(Region)) ||UPPER(TRIM(JnJRKAM)) ||UPPER(TRIM(JnJZM_Code)) ||UPPER(TRIM(JNJ_ABI_Code)) ||UPPER(TRIM(JnJSupervisor_Code)) ||UPPER(TRIM(ISP_Code)) ||UPPER(TRIM(ISP_Name)) ||UPPER(TRIM(MONTH)) ||UPPER(TRIM(YEAR)) ||UPPER(TRIM(Format)) ||UPPER(TRIM(Chain_Code)) ||UPPER(TRIM(Chain)) ||UPPER(TRIM(Store_Code)) ||UPPER(TRIM(Store_Name)) ||UPPER(TRIM(Asset)) ||UPPER(TRIM(Product_Category)) ||UPPER(TRIM(Product_Brand)) ||UPPER(TRIM(POSM_Brand)) ||UPPER(TRIM(Start_Date)) ||UPPER(TRIM(End_Date)) ||UPPER(TRIM(Audit_Status)) ||UPPER(TRIM(Is_Available)) 
        ||UPPER(TRIM(Availability_Points)) ||UPPER(TRIM(Visibility_Type)) ||UPPER(TRIM(Visibility_Condition)) ||UPPER(TRIM(Is_Planogram_Availbale)) ||UPPER(TRIM(Select_Brand)) ||UPPER(TRIM(Is_Correct_Brand_Displayed)) ||UPPER(TRIM(BrandAvailability_Points)) 
        ||UPPER(TRIM(Stock_Status)) ||UPPER(TRIM(Stock_Points)) ||UPPER(TRIM(Is_Near_Category)) ||UPPER(TRIM(NearCategory_Points)) ||UPPER(TRIM(Audit_Score)) ||UPPER(TRIM(Paid_Visibility_Score)) ||UPPER(TRIM(Reason)) ||UPPER(TRIM(Priority_Store))
        FROM 
        (SELECT Visit_ID,
                CASE
                WHEN REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-3][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]') OR REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]') 
                THEN TO_DATE(Visit_DateTime,'DD-MON-YY')
                WHEN REGEXP_LIKE (UPPER(TRIM(Visit_DateTime)),'[0-3][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9][0-9][0-9]') OR REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9][0-9][0-9]')
                THEN TO_DATE(Visit_DateTime,'DD-MON-YYYY')
                WHEN REGEXP_LIKE(TRIM(Visit_DateTime),'[0-1][0-9]-[0-3][0-9]-[1-2][0-9]') OR REGEXP_LIKE(TRIM(Visit_DateTime),'[0-9]-[0-3][0-9]-[1-2][0-9]') OR REGEXP_LIKE(TRIM(Visit_DateTime),'[0-1][0-9]-[0-9]-[1-2][0-9]') OR REGEXP_LIKE(TRIM(Visit_DateTime),'[0-9]-[0-9]-[1-2][0-9]') 
                THEN TO_DATE(Visit_DateTime,'MM-DD-YY')
                ELSE TO_DATE(TRIM(Visit_DateTime),'MM-DD-YYYY')
            END AS Visit_DateTime,
            Region,
            JnJRKAM,
            JnJZM_Code,
            JNJ_ABI_Code,
            JnJSupervisor_Code,
            ISP_Code,
            ISP_Name,
            MONTH,
            YEAR,
            Format,
            Chain_Code,
            Chain,
            Store_Code,
            Store_Name,
            Asset,
            Product_Category,
            Product_Brand,
            POSM_Brand,
            Start_Date,
            End_Date,
            Audit_Status,
            Is_Available,
            Availability_Points,
            Visibility_Type,
            Visibility_Condition,
            Is_Planogram_Availbale,
            Select_Brand,
            Is_Correct_Brand_Displayed,
            BrandAvailability_Points,
            Stock_Status,
            Stock_Points,
            Is_Near_Category,
            NearCategory_Points,
            Audit_Score,
            Paid_Visibility_Score,
            Reason,
            Priority_Store
        FROM {{ source('snapindsdl_raw', 'sdl_in_perfectstore_paid_display') }}
        ));
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('snapindsdl_raw', 'sdl_in_perfectstore_paid_display') }}
),
final as
(
    SELECT Visit_ID,
            CASE
            WHEN REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-3][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]') OR REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]') 
            THEN TO_DATE(Visit_DateTime,'DD-MON-YY')
            WHEN REGEXP_LIKE (UPPER(TRIM(Visit_DateTime)),'[0-3][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9][0-9][0-9]') OR REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9][0-9][0-9]')
            THEN TO_DATE(Visit_DateTime,'DD-MON-YYYY')
            WHEN REGEXP_LIKE(TRIM(Visit_DateTime),'[0-1][0-9]-[0-3][0-9]-[1-2][0-9]') OR REGEXP_LIKE(TRIM(Visit_DateTime),'[0-9]-[0-3][0-9]-[1-2][0-9]') OR REGEXP_LIKE(TRIM(Visit_DateTime),'[0-1][0-9]-[0-9]-[1-2][0-9]') OR REGEXP_LIKE(TRIM(Visit_DateTime),'[0-9]-[0-9]-[1-2][0-9]') 
            THEN TO_DATE(Visit_DateTime,'MM-DD-YY')
            ELSE TO_DATE(TRIM(Visit_DateTime),'MM-DD-YYYY')
        END AS Visit_DateTime,
        Region,
        JnJRKAM,
        JnJZM_Code,
        JNJ_ABI_Code,
        JnJSupervisor_Code,
        ISP_Code,
        ISP_Name,
        MONTH,
        YEAR,
        Format,
        Chain_Code,
        Chain,
        Store_Code,
        Store_Name,
        Asset,
        Product_Category,
        Product_Brand,
        posm_brand,
        Start_Date,
        End_Date,
        Audit_Status,
        Is_Available,
        Availability_Points,
        Visibility_Type,
        Visibility_Condition,
        Is_Planogram_Availbale,
        Select_Brand,
        Is_Correct_Brand_Displayed,
        BrandAvailability_Points,
        Stock_Status,
        Stock_Points,
        Is_Near_Category,
        NearCategory_Points,
        Audit_Score,
        Paid_Visibility_Score,
        Reason,
        Priority_Store,
        run_id,
        file_name,
        yearmo,
        convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz AS crtd_dttm
    FROM source
)
select visit_id::varchar(255) as visit_id,
    visit_datetime::timestamp_ntz(9) as visit_datetime,
    region::varchar(255) as region,
    jnjrkam::varchar(255) as jnjrkam,
    jnjzm_code::varchar(255) as jnjzm_code,
    jnj_abi_code::varchar(255) as jnj_abi_code,
    jnjsupervisor_code::varchar(255) as jnjsupervisor_code,
    isp_code::varchar(255) as isp_code,
    isp_name::varchar(255) as isp_name,
    month::varchar(255) as month,
    year::number(10,0) as year,
    format::varchar(255) as format,
    chain_code::varchar(255) as chain_code,
    chain::varchar(255) as chain,
    store_code::varchar(255) as store_code,
    store_name::varchar(255) as store_name,
    asset::varchar(255) as asset,
    product_category::varchar(255) as product_category,
    product_brand::varchar(255) as product_brand,
    posm_brand::varchar(255) as posm_brand,
    start_date::varchar(255) as start_date,
    end_date::varchar(255) as end_date,
    audit_status::varchar(255) as audit_status,
    is_available::varchar(255) as is_available,
    availability_points::varchar(255) as availability_points,
    visibility_type::varchar(255) as visibility_type,
    visibility_condition::varchar(255) as visibility_condition,
    is_planogram_availbale::varchar(255) as is_planogram_availbale,
    select_brand::varchar(255) as select_brand,
    is_correct_brand_displayed::varchar(255) as is_correct_brand_displayed,
    brandavailability_points::varchar(255) as brandavailability_points,
    stock_status::varchar(255) as stock_status,
    stock_points::varchar(255) as stock_points,
    is_near_category::varchar(255) as is_near_category,
    nearcategory_points::varchar(255) as nearcategory_points,
    audit_score::varchar(255) as audit_score,
    paid_visibility_score::varchar(255) as paid_visibility_score,
    reason::varchar(255) as reason,
    priority_store::varchar(255) as priority_store,
    run_id::number(14,0) as run_id,
    file_name::varchar(255) as file_name,
    yearmo::varchar(255) as yearmo,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm
from final