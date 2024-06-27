{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE UPPER(TRIM(Visit_ID) )||UPPER(TRIM(LEFT(Visit_DateTime,10)))||UPPER(TRIM(Region) )||UPPER(TRIM(JnJRKAM) )||UPPER(TRIM(JnJZM_Code) )||UPPER(TRIM(JNJ_ABI_Code) )||UPPER(TRIM(JnJSupervisor_Code) )||UPPER(TRIM(ISP_Code) )||UPPER(TRIM(ISP_Name) )||UPPER(TRIM(MONTH) 
        )||UPPER(TRIM(YEAR) )||UPPER(TRIM(Format) )||UPPER(TRIM(Chain_Code) )||UPPER(TRIM(Chain) )||UPPER(TRIM(Store_Code) )||UPPER(TRIM(Store_Name) )||UPPER(TRIM(Product_Code) )||UPPER(TRIM(Product_Name) )||UPPER(TRIM(MSL) )||UPPER(TRIM(Cost_INR) )||UPPER(TRIM(Quantity) 
        )||UPPER(TRIM(Amount_INR) )||UPPER(TRIM(Priority_Store))
        IN 
        (SELECT DISTINCT UPPER(TRIM(Visit_ID) )||UPPER(TRIM(Visit_DateTime) )||UPPER(TRIM(Region) )||UPPER(TRIM(JnJRKAM) )||UPPER(TRIM(JnJZM_Code) )||UPPER(TRIM(JNJ_ABI_Code) )||UPPER(TRIM(JnJSupervisor_Code) )||UPPER(TRIM(ISP_Code) )||UPPER(TRIM(ISP_Name) )||UPPER(TRIM(MONTH) 
        )||UPPER(TRIM(YEAR) )||UPPER(TRIM(Format) )||UPPER(TRIM(Chain_Code) )||UPPER(TRIM(Chain) )||UPPER(TRIM(Store_Code) )||UPPER(TRIM(Store_Name) )||UPPER(TRIM(Product_Code) )||UPPER(TRIM(Product_Name) )||UPPER(TRIM(MSL) )||UPPER(TRIM(Cost_INR) )||UPPER(TRIM(Quantity) 
        )||UPPER(TRIM(Amount_INR) )||UPPER(TRIM(Priority_Store) )
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
            Product_Code,
            Product_Name,
            MSL,
            Cost_INR,
            Quantity,
            Amount_INR,
            Priority_Store
        FROM {{ source('snapindsdl_raw', 'sdl_in_perfectstore_msl') }}));
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('snapindsdl_raw', 'sdl_in_perfectstore_msl') }}
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
        Product_Code,
        Product_Name,
        MSL,
        Cost_INR,
        Quantity,
        Amount_INR,
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
    product_code::varchar(255) as product_code,
    product_name::varchar(255) as product_name,
    msl::varchar(255) as msl,
    cost_inr::number(12,4) as cost_inr,
    quantity::number(10,0) as quantity,
    amount_inr::number(12,4) as amount_inr,
    priority_store::varchar(255) as priority_store,
    run_id::number(14,0) as run_id,
    file_name::varchar(255) as file_name,
    yearmo::varchar(255) as yearmo,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm
from final