with itg_hcp360_sfmc_hcp_details as
(
    select * from dev_dna_core.snapinditg_integration.itg_hcp360_sfmc_hcp_details
),
edw_hcp360_hcp_master_key_by_brand as
(
    select * from dev_dna_core.snapindedw_integration.edw_hcp360_hcp_master_key_by_brand
),
mas_key1 as
(
    SELECT DISTINCT SUBSCRIBER_KEY,
        MASTER_HCP_KEY,
        MOBILE_PHONE,
        PERSON_EMAIL,
        row_number() OVER (
            PARTITION BY subscriber_key ORDER BY account_source_id,
                ventasys_custid DESC
            ) rn
    FROM EDW_HCP360_HCP_MASTER_KEY_BY_BRAND
),
mas_key as
(
    SELECT SUBSCRIBER_KEY,
        MASTER_HCP_KEY
    FROM MAS_KEY1
    WHERE rn = 1
),
transformed as
(
    SELECT MD5(HCP.SUBSCRIBER_KEY || nvl(HCP.COUNTRY, split_part(HCP.SUBSCRIBER_KEY, '_', 2))) AS HCP_ID,
        MAS_KEY.MASTER_HCP_KEY,
        HCP.SUBSCRIBER_KEY,
        COUNTRY,
        BRAND,
        FIRST_NAME,
        LAST_NAME,
        MOBILE_NUMBER,
        MOBILE_COUNTRY_CODE,
        ADDRESS_CITY,
        ADDRESS_ZIPCODE,
        CREATED_DATE,
        UPDATED_DATE,
        UNSUBSCRIBE_DATE,
        STATUS,
        PROFESSION,
        SPECIALTY,
        LICENSE_NUMBER,
        LICENSING_CITY,
        TERRITORY_NAME,
        POSITION,
        EMAIL,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as CRT_DTTM,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as UPDT_DTTM
    FROM ITG_HCP360_SFMC_HCP_DETAILS HCP
    LEFT JOIN mas_key ON HCP.SUBSCRIBER_KEY = MAS_KEY.SUBSCRIBER_KEY
),
final as 
(   
    select
        hcp_id::varchar(32) as hcp_id,
        master_hcp_key::varchar(32) as master_hcp_key,
        subscriber_key::varchar(100) as subscriber_key,
        country::varchar(20) as country,
        brand::varchar(50) as brand,
        first_name::varchar(30) as first_name,
        last_name::varchar(30) as last_name,
        mobile_number::varchar(20) as mobile_number,
        mobile_country_code::varchar(5) as mobile_country_code,
        address_city::varchar(20) as address_city,
        address_zipcode::number(10,0) as address_zipcode,
        created_date::timestamp_ntz(9) as created_date,
        updated_date::timestamp_ntz(9) as updated_date,
        unsubscribe_date::timestamp_ntz(9) as unsubscribe_date,
        status::varchar(10) as status,
        profession::varchar(20) as profession,
        specialty::varchar(30) as specialty,
        license_number::varchar(20) as license_number,
        licensing_city::varchar(20) as licensing_city,
        territory_name::varchar(30) as territory_name,
        position::varchar(20) as position,
        email::varchar(50) as email,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    from transformed 
)
select * from final
