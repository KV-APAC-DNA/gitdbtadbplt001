with source as (
    select * from {{ source('myssdl_raw','sdl_mds_my_gt_outletattributes') }}
),
final as (
    select
        distributor_id::varchar(20) as cust_id,
        distributor_name::varchar(100) as cust_nm,
        customer_code::varchar(25) as outlet_id,
        customer_name1::varchar(100) as outlet_desc,
        outlet_type1::varchar(50) as outlet_type1,
        outlet_type2::varchar(50) as outlet_type2,
        outlet_type3::varchar(100) as outlet_type3,
        outlet_type4::varchar(100) as outlet_type4,
        town::varchar(50) as town,
        cust_year::varchar(50) as cust_year,
        salesman_code::varchar(30) as slsmn_cd,
        null::varchar(50) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final