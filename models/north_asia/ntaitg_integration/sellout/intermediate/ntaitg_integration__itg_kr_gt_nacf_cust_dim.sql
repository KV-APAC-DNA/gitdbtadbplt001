{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= " {% if is_incremental() %}
                    -- delete from {{this}} where (nacf_customer_code, sap_customer_code) in (select code as nacf_customer_code, cast(customer_code as varchar) as sap_customer_code from {{ source('ntasdl_raw','sdl_mds_kr_sub_customer_master') }} where trim(upper(retailer_code)) = 'NACF');
                    delete from {{this}} where (nacf_customer_code, sap_customer_code) in (select code as nacf_customer_code, cast(sap_customer_code as varchar) as sap_customer_code from {{ source('ntasdl_raw','sdl_mds_kr_sub_customer_master') }} where trim(upper(replace(retailer, '{}', ''))) = 'NACF');
                    {% endif %}
                    "
    )
}}
with source as (
    select * from {{ source('ntasdl_raw','sdl_mds_kr_sub_customer_master') }}
),
final as (
    select 
        'KR'::varchar(2) as cntry_cd ,
        -- retailer_code::varchar(30) as dstr_nm,
        replace(retailer, '{}', '') as dstr_nm,
        code::varchar(50) as nacf_customer_code,
        -- customer_code::varchar(50) as sap_customer_code,
        CAST(sap_customer_code AS VARCHAR) AS sap_customer_code,
        current_timestamp()::timestamp_ntz(9) as created_dt
    from source
    -- where trim(upper(retailer_code)) = 'NACF'
    WHERE UPPER(TRIM(replace(retailer, '{}', ''))) = 'NACF'
)
select * from  final
