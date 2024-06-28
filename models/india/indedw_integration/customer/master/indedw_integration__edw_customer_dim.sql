{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["customer_code","chng_flg"],
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} a using {{ ref('indwks_integration__wks_customer_dim') }} b
        where a.customer_code = b.customer_code
        and b.chng_flg = 'U';
        {% endif %}"
    )
}}

with source as 
(
    select * from {{ ref('indwks_integration__wks_customer_dim') }}
),
final as 
(
    select customer_code::varchar(20) as customer_code,
        region_code::number(18,0) as region_code,
        region_name::varchar(50) as region_name,
        zone_code::number(18,0) as zone_code,
        zone_name::varchar(50) as zone_name,
        zone_classification::varchar(50) as zone_classification,
        territory_code::number(18,0) as territory_code,
        territory_name::varchar(50) as territory_name,
        territory_classification::varchar(50) as territory_classification,
        state_code::number(18,0) as state_code,
        state_name::varchar(50) as state_name,
        town_name::varchar(50) as town_name,
        town_classification::varchar(100) as town_classification,
        city::varchar(150) as city,
        type_code::number(18,0) as type_code,
        type_name::varchar(50) as type_name,
        customer_name::varchar(150) as customer_name,
        customer_address1::varchar(150) as customer_address1,
        customer_address2::varchar(150) as customer_address2,
        customer_address3::varchar(150) as customer_address3,
        active_flag::varchar(7) as active_flag,
        active_start_date::timestamp_ntz(9) as active_start_date,
        wholesalercode::varchar(50) as wholesalercode,
        super_stockiest::varchar(50) as super_stockiest,
        direct_account_flag::varchar(7) as direct_account_flag,
        abi_code::number(18,0) as abi_code,
        abi_name::varchar(101) as abi_name,
        rds_size::varchar(100) as rds_size,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        ''::varchar(20) as num_of_retailers,
        ''::varchar(255) as customer_type,
        psnonps::varchar(1) as psnonps,
        suppliedby::number(18,0) as suppliedby,
        cfa::varchar(50) as cfa,
        cfa_name::varchar(50) as cfa_name,
        town_code::varchar(50) as town_code
        from source
)
select * from final