with source as (
    select * from {{ source('myssdl_raw', 'sdl_my_dstrbtrr_dim') }} where file_name not in
    ( select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_dstrbtrr_dim__null_test') }}
      union all 
      select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_dstrbtrr_dim__duplicate_test') }}
    )
),
final as (
    select
        cust_id::varchar(10) as cust_id,
        cust_nm::varchar(100) as cust_nm,
        lvl1::varchar(40) as lvl1,
        lvl2::varchar(40) as lvl2,
        lvl3::varchar(40) as lvl3,
        lvl4::varchar(40) as lvl4,
        lvl5::varchar(40) as lvl5,
        trdng_term_val::number(20,4) as trdng_term_val,
        abbrevation::varchar(40) as abbrevation,
        buyer_gln::varchar(40) as buyer_gln,
        location_gln::varchar(40) as location_gln,
        chnl_manager::varchar(80) as chnl_manager,
        cdm::varchar(40) as cdm,
        region::varchar(20) as region,
        cdl_dttm::varchar(255) as cdl_dttm,
        curr_dt::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        file_name::varchar(255) as file_name
    from source
)

select * from final