{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if is_incremental() %}
        -- delete from {{this}} where (customer_code, coalesce(sub_customer_code, 'NA')) in (select cast(customer_code as varchar) as customer_code, 'NA' as sub_customer_code from {{ source('ntasdl_raw', 'sdl_mds_kr_sub_customer_master') }} where upper(trim(retailer_code)) = 'FOOD_WS');
        delete from {{this}} where (customer_code, coalesce(sub_customer_code, 'NA')) in (select cast(sap_customer_code as varchar) as customer_code, 'NA' as sub_customer_code from {{ source('ntasdl_raw', 'sdl_mds_kr_sub_customer_master') }} where upper(trim(replace(retailer, '{}', ''))) = 'FOOD_WS');
        {% endif %}"
    )
}}
with sdl_mds_kr_sub_customer_master as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_sub_customer_master') }}
),
transformed as (
    SELECT 
        'KR' as cntry_cd,
        -- RETAILER_CODE AS DSTR_NM,
        replace(retailer, '{}', '') as dstr_nm,
        NAME AS SUB_CUSTOMER_NAME,
        'NA' AS SUB_CUSTOMER_CODE,
        -- CAST(customer_code AS VARCHAR) AS CUSTOMER_CODE,
        CAST(sap_customer_code AS VARCHAR) AS CUSTOMER_CODE,
        current_timestamp() as created_dt
    FROM SDL_MDS_KR_SUB_CUSTOMER_MASTER
    -- WHERE UPPER(TRIM(RETAILER_CODE)) = 'FOOD_WS'
    WHERE UPPER(TRIM(replace(retailer, '{}', ''))) = 'FOOD_WS'
),
final as (
    select
    cntry_cd::varchar(2) as cntry_cd,
    DSTR_NM::varchar(20) as sales_grp,
    sub_customer_name::varchar(100) as sub_customer_name,
    sub_customer_code::varchar(50) as sub_customer_code,
    customer_code::varchar(50) as customer_code,
    created_dt::timestamp_ntz(9) as created_dt
    from transformed
)
select * from  final
