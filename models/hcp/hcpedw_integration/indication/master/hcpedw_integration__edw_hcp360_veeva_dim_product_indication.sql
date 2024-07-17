{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = ["{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE PRODUCT_INDICATION_KEY IN (SELECT PRODUCT_INDICATION_KEY
                                 FROM {{ source('hcpitg_integration', 'itg_hcp360_veeva_product') }} ITG_PROD
                                 WHERE ITG_PROD.PRODUCT_INDICATION_KEY = PRODUCT_INDICATION_KEY);
        {% endif %}", "{% if is_incremental() %} 
        DELETE FROM {{this}}
        WHERE PRODUCT_INDICATION_KEY = 'Not Applicable';
        {% endif %}"]
    )
}}

with itg_hcp360_veeva_product as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_product') }}
),
itg_hcp360_veeva_recordtype as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_recordtype') }}
),
final as
(
    SELECT DISTINCT PRODUCT_INDICATION_KEY,
    ITG_PROD.COUNTRY_CODE,
    NVL(ITG_PROD.PRODUCT_SOURCE_ID,'Not Applicable') as PRODUCT_SOURCE_ID,
    ITG_PROD.LAST_MODIFIED_DATE as MODIFY_DT,
    NVL(ITG_PROD.LAST_MODIFIED_BY_ID,'Not Applicable') as MODIFY_ID,
    ITG_PROD.PRODUCT_NAME,
    ITG_PROD.PRODUCT_TYPE,
    THERAPEUTIC_AREA AS THERAPEUTIC_AREA_NAME,
    PARENT_PRODUCT_KEY,
    COMPANY_PRODUCT AS COMPANY_PRODUCT_FLAG,
    BUSINESS_UNIT,
    CONSUMER_SITE,
    PRODUCT_INFO,
    THERAPEUTIC_CLASS,
    MANUFACTURER,
    ITG_PROD.DESCRIPTION,
    FRANCHISE,
    REQUIRE_KEY_MESSAGE,
    CONTROLLED_SUBSTANCE,
    SAMPLE_QUANTITY_PICKLIST,
    ITG_PROD.DISPLAY_ORDER,
    NO_METRICS,
    DISTRIBUTOR,
    SAMPLE_U_M,
    NO_DETAILS,
    RESTRICTED,
    NO_CYCLE_PLANS,
    SKU_ID,
    BIZ_SUB_UNIT,
    BIZ_UNIT,
    PRODUCT_SECTOR,
    NVL(ITG_PROD.EXTERNAL_ID,'Not Applicable') as EXTERNAL_ID,
    NVL(ITG_PROD.IS_DELETED,0) as DELETED_FLAG,
    ITG_REC.RECORD_TYPE_NAME,
    ITG_PROD.SHC_Sector,
    ITG_PROD.SHC_Strategic_Group,
    ITG_PROD.SHC_Franchise,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as CRT_DTTM,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as UPDT_DTTM
    FROM itg_hcp360_veeva_product ITG_PROD
    LEFT OUTER JOIN itg_hcp360_veeva_recordtype ITG_REC
    ON ITG_PROD.RECORD_TYPE_SOURCE_ID = ITG_REC.RECORD_TYPE_SOURCE_ID
    AND ITG_PROD.COUNTRY_CODE = ITG_REC.COUNTRY_CODE


    UNION ALL

    SELECT 'Not Applicable','ZZ','Not Applicable',convert_timezone('UTC',current_timestamp())::timestamp_ntz,
    'Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable',0,'Not Applicable',
    'Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable','Not Applicable',
    0,0,'Not Applicable',0,0,'Not Applicable','Not Applicable',0,0,0,'Not Applicable','Not Applicable',
    'Not Applicable','Not Applicable','Not Applicable',0,'Not Applicable','Not Applicable','Not Applicable',
    'Not Applicable',convert_timezone('UTC',current_timestamp())::timestamp_ntz,convert_timezone('UTC',current_timestamp())::timestamp_ntz
)
select product_indication_key::varchar(32) as product_indication_key,
    country_code::varchar(8) as country_code,
    product_source_id::varchar(18) as product_source_id,
    modify_dt::timestamp_ntz(9) as modify_dt,
    modify_id::varchar(18) as modify_id,
    product_name::varchar(80) as product_name,
    product_type::varchar(255) as product_type,
    therapeutic_area_name::varchar(255) as therapeutic_area_name,
    parent_product_key::varchar(80) as parent_product_key,
    company_product_flag::number(18,0) as company_product_flag,
    business_unit::varchar(4099) as business_unit,
    consumer_site::varchar(255) as consumer_site,
    product_info::varchar(255) as product_info,
    therapeutic_class::varchar(255) as therapeutic_class,
    manufacturer::varchar(255) as manufacturer,
    description::varchar(255) as description,
    franchise::varchar(4099) as franchise,
    require_key_message::number(18,0) as require_key_message,
    controlled_substance::number(18,0) as controlled_substance,
    sample_quantity_picklist::varchar(1030) as sample_quantity_picklist,
    display_order::number(5,0) as display_order,
    no_metrics::number(18,0) as no_metrics,
    distributor::varchar(255) as distributor,
    sample_u_m::varchar(255) as sample_u_m,
    no_details::number(18,0) as no_details,
    restricted::number(18,0) as restricted,
    no_cycle_plans::number(18,0) as no_cycle_plans,
    sku_id::varchar(25) as sku_id,
    biz_sub_unit::varchar(255) as biz_sub_unit,
    biz_unit::varchar(255) as biz_unit,
    product_sector::varchar(1030) as product_sector,
    external_id::varchar(25) as external_id,
    record_type_name::varchar(80) as record_type_name,
    deleted_flag::number(18,0) as deleted_flag,
    shc_sector::varchar(255) as shc_sector,
    shc_strategic_group::varchar(255) as shc_strategic_group,
    shc_franchise::varchar(255) as shc_franchise,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final