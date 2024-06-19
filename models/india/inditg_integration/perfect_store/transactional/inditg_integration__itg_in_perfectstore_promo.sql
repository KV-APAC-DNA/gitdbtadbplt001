{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE UPPER(TRIM(Visit_ID)) ||UPPER(TRIM(LEFT(Visit_DateTime,10))) ||UPPER(TRIM(Region)) ||UPPER(TRIM(JnJRKAM)) ||UPPER(TRIM(JnJZM_Code)) ||UPPER(TRIM(JNJ_ABI_Code)) ||UPPER(TRIM(JnJSupervisor_Code)) ||UPPER(TRIM(ISP_Code)) ||UPPER(TRIM(ISP_Name)) ||UPPER(TRIM(MONTH)) ||UPPER(TRIM(YEAR)) ||UPPER(TRIM(Format)) 
        ||UPPER(TRIM(Chain_Code)) ||UPPER(TRIM(Chain)) ||UPPER(TRIM(Store_Code)) ||UPPER(TRIM(Store_Name)) ||UPPER(TRIM(Product_Category)) ||UPPER(TRIM(Product_Brand)) ||UPPER(TRIM(Promotion_Product_Code)) ||UPPER(TRIM(Promotion_Product_Name)) ||UPPER(TRIM(IsPromotionAvailable)) ||UPPER(TRIM(Priority_Store))
        IN 
        (SELECT DISTINCT UPPER(TRIM(Visit_ID)) || UPPER(TRIM(Visit_DateTime))|| UPPER(TRIM(Region))||UPPER(TRIM(JnJRKAM)) || UPPER(TRIM(JnJZM_Code)) || UPPER(TRIM(JNJ_ABI_Code)) || UPPER(TRIM(JnJSupervisor_Code)) || UPPER(TRIM(ISP_Code)) || UPPER(TRIM(ISP_Name)) || UPPER(TRIM(MONTH)) || UPPER(TRIM(YEAR))|| UPPER(TRIM(Format)) 
        || UPPER(TRIM(Chain_Code)) || UPPER(TRIM(Chain)) || UPPER(TRIM(Store_Code)) || UPPER(TRIM(Store_Name)) || UPPER(TRIM(Product_Category)) || UPPER(TRIM(Product_Brand)) || UPPER(TRIM(Promotion_Product_Code)) || UPPER(TRIM(Promotion_Product_Name)) || UPPER(TRIM(IsPromotionAvailable)) || UPPER(TRIM(Priority_Store))
        FROM 
        (SELECT Visit_ID,
                CASE
                WHEN REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-3][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]') OR REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]') 
                THEN TO_DATE(Visit_DateTime,'DD-MON-YY')
                WHEN REGEXP_LIKE (UPPER(TRIM(Visit_DateTime)),'[0-3][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]%') OR REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]%')
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
            Product_Category,
            Product_Brand,
            Promotion_Product_Code,
            Promotion_Product_Name,
            IsPromotionAvailable,
            PhotoPath,
            CountOfFacing,
            PromotionOfferType,
            NotAvailableReason,
            Price_Off,
            Priority_Store
        FROM {{ source('snapindsdl_raw', 'sdl_in_perfectstore_promo') }}));
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('snapindsdl_raw', 'sdl_in_perfectstore_promo') }}
),
final as
(
    SELECT Visit_ID,
            CASE
            WHEN REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-3][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]') OR REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]') 
            THEN TO_DATE(Visit_DateTime,'DD-MON-YY')
            WHEN REGEXP_LIKE (UPPER(TRIM(Visit_DateTime)),'[0-3][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]%') OR REGEXP_LIKE(UPPER(TRIM(Visit_DateTime)),'[0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-[1-2][0-9]%')
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
        Product_Category,
        Product_Brand,
        Promotion_Product_Code,
        Promotion_Product_Name,
        IsPromotionAvailable,
        PhotoPath,
        CountOfFacing,
        PromotionOfferType,
        NotAvailableReason,
        Price_Off,
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
    product_category::varchar(255) as product_category,
    product_brand::varchar(255) as product_brand,
    promotion_product_code::varchar(255) as promotion_product_code,
    promotion_product_name::varchar(255) as promotion_product_name,
    ispromotionavailable::varchar(255) as ispromotionavailable,
    photopath::varchar(500) as photopath,
    countoffacing::number(10,0) as countoffacing,
    promotionoffertype::varchar(255) as promotionoffertype,
    notavailablereason::varchar(255) as notavailablereason,
    price_off::varchar(255) as price_off,
    priority_store::varchar(255) as priority_store,
    run_id::number(14,0) as run_id,
    file_name::varchar(255) as file_name,
    yearmo::varchar(255) as yearmo,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm
from final