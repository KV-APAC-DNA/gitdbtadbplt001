{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE UPPER(TRIM(LEFT(Visit_DateTime,10)) )||UPPER(TRIM(Region) )||UPPER(TRIM(JnJRKAM) )||UPPER(TRIM(JnJZM_Code) )||UPPER(TRIM(JNJ_ABI_Code) )||UPPER(TRIM(JnJSupervisor_Code) )||UPPER(TRIM(ISP_Code) )||UPPER(TRIM(JnJISP_Name) )||UPPER(TRIM(MONTH) )||UPPER(TRIM(YEAR) 
        )||UPPER(TRIM(Format) )||UPPER(TRIM(Chain_Code) )||UPPER(TRIM(Chain) )||UPPER(TRIM(Store_Code) )||UPPER(TRIM(Store_Name) )||UPPER(TRIM(Category) )||UPPER(TRIM(Prod_Facings) )||UPPER(TRIM(Total_Facings) )|| UPPER(TRIM(Priority_Store))
        IN 
        (SELECT DISTINCT UPPER(TRIM(Visit_DateTime) )||UPPER(TRIM(Region) )||UPPER(TRIM(JnJRKAM) )||UPPER(TRIM(JnJZM_Code) )||UPPER(TRIM(JNJ_ABI_Code) )||UPPER(TRIM(JnJSupervisor_Code) )||UPPER(TRIM(ISP_Code) )||UPPER(TRIM(JnJISP_Name) )||UPPER(TRIM(MONTH) )||UPPER(TRIM(YEAR) 
        )||UPPER(TRIM(Format) )||UPPER(TRIM(Chain_Code) )||UPPER(TRIM(Chain) )||UPPER(TRIM(Store_Code) )||UPPER(TRIM(Store_Name) )||UPPER(TRIM(Category) )||UPPER(TRIM(Prod_Facings) )||UPPER(TRIM(Total_Facings) )|| UPPER(TRIM(Priority_Store))
        FROM 
        (SELECT CASE
                WHEN REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-3][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]') OR REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]') 
                THEN TO_DATE(Visit_DateTime,'YYYY-MM-DD')
                WHEN REGEXP_LIKE (UPPER(TRIM(Visit_DateTime)),'[0-3][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9][0-9][0-9]') OR REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9][0-9][0-9]')
                THEN TO_DATE(Visit_DateTime,'YYYY-MM-DD')
                WHEN REGEXP_LIKE(TRIM(Visit_DateTime),'[0-1][0-9]-[0-3][0-9]-[1-2][0-9]') OR REGEXP_LIKE(TRIM(Visit_DateTime),'[0-9]-[0-3][0-9]-[1-2][0-9]') OR REGEXP_LIKE(TRIM(Visit_DateTime),'[0-1][0-9]-[0-9]-[1-2][0-9]') OR REGEXP_LIKE(TRIM(Visit_DateTime),'[0-9]-[0-9]-[1-2][0-9]') 
                THEN TO_DATE(Visit_DateTime,'YYYY-MM-DD')
                ELSE TO_DATE(TRIM(Visit_DateTime),'YYYY-MM-DD')
            END AS Visit_DateTime,
            Region,
            JnJRKAM,
            JnJZM_Code,
            JNJ_ABI_Code,
            JnJSupervisor_Code,
            ISP_Code,
            JnJISP_Name,
            MONTH,
            YEAR,
            Format,
            Chain_Code,
            Chain,
            Store_Code,
            Store_Name,
            Category,
            Prod_Facings,
            Total_Facings,
            Priority_Store
        FROM {{ source('indsdl_raw', 'sdl_in_perfectstore_sos') }}));
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('indsdl_raw', 'sdl_in_perfectstore_sos') }}
    where file_name not in (
        select distinct file_name from {{source('indwks_integration','TRATBL_sdl_in_perfectstore_sos__null_test')}}
        union all
        select distinct file_name from {{source('indwks_integration','TRATBL_sdl_in_perfectstore_sos__duplicate_test')}}
        union all
        select distinct file_name from {{source('indwks_integration','TRATBL_sdl_in_perfectstore_sos__date_format_test')}})
),
final as
(
    SELECT CASE
            WHEN REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-3][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]') OR REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]') 
            THEN TO_DATE(Visit_DateTime,'YYYY-MM-DD')
            WHEN REGEXP_LIKE (UPPER(TRIM(Visit_DateTime)),'[0-3][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9][0-9][0-9]') OR REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9][0-9][0-9]')
            THEN TO_DATE(Visit_DateTime,'YYYY-MM-DD')
            WHEN REGEXP_LIKE(TRIM(Visit_DateTime),'[0-1][0-9]-[0-3][0-9]-[1-2][0-9]') OR REGEXP_LIKE(TRIM(Visit_DateTime),'[0-9]-[0-3][0-9]-[1-2][0-9]') OR REGEXP_LIKE(TRIM(Visit_DateTime),'[0-1][0-9]-[0-9]-[1-2][0-9]') OR REGEXP_LIKE(TRIM(Visit_DateTime),'[0-9]-[0-9]-[1-2][0-9]') 
            THEN TO_DATE(Visit_DateTime,'YYYY-MM-DD')
            ELSE TO_DATE(TRIM(Visit_DateTime),'YYYY-MM-DD')
        END AS Visit_DateTime,
        Region,
        JnJRKAM,
        JnJZM_Code,
        JNJ_ABI_Code,
        JnJSupervisor_Code,
        ISP_Code,
        JnJISP_Name,
        MONTH,
        YEAR,
        Format,
        Chain_Code,
        Chain,
        Store_Code,
        Store_Name,
        Category,
        Prod_Facings,
        Total_Facings,
        Facing_Contribution,
        Priority_Store,
        run_id,
        file_name,
        yearmo,
        convert_timezone('Asia/Singapore',current_timestamp())::timestamp_ntz AS crtd_dttm
    FROM source
)
select visit_datetime::timestamp_ntz(9) as visit_datetime,
    region::varchar(255) as region,
    jnjrkam::varchar(255) as jnjrkam,
    jnjzm_code::varchar(255) as jnjzm_code,
    jnj_abi_code::varchar(255) as jnj_abi_code,
    jnjsupervisor_code::varchar(255) as jnjsupervisor_code,
    isp_code::varchar(255) as isp_code,
    jnjisp_name::varchar(255) as jnjisp_name,
    month::varchar(255) as month,
    year::number(10,0) as year,
    format::varchar(255) as format,
    chain_code::varchar(255) as chain_code,
    chain::varchar(255) as chain,
    store_code::varchar(255) as store_code,
    store_name::varchar(255) as store_name,
    category::varchar(255) as category,
    prod_facings::number(14,0) as prod_facings,
    total_facings::number(14,0) as total_facings,
    facing_contribution::varchar(255) as facing_contribution,
    priority_store::varchar(255) as priority_store,
    run_id::number(14,0) as run_id,
    file_name::varchar(255) as file_name,
    yearmo::varchar(255) as yearmo,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm
from final