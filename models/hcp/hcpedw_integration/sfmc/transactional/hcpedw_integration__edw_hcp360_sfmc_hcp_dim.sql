with wks_sfmc_hcp_dim as 
(
    select * from dev_dna_core.snapindwks_integration.wks_sfmc_hcp_dim
),
final as 
(
    select
        hcp_id::varchar(50) as hcp_id,
        master_hcp_key::varchar(50) as hcp_master_id,
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
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from wks_sfmc_hcp_dim
)
select * from final 
