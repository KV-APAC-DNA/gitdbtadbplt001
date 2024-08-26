with source as (
    select * from {{ source('myssdl_raw','sdl_my_pos_cust_mstr') }} where file_name not in
    ( select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_pos_cust_mstr__duplicate_test') }}
    )
),

final as (
    select
        cust_id::varchar(10) as cust_id,
        cust_nm::varchar(255) as cust_nm,
        store_cd::varchar(10) as store_cd,
        store_nm::varchar(255) as store_nm,
        dept_cd::varchar(10) as dept_cd,
        dept_nm::varchar(255) as dept_nm,
        region::varchar(255) as region,
        store_frmt::varchar(255) as store_frmt,
        store_type::varchar(255) as store_type,
        cdl_dttm::varchar(50) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final
