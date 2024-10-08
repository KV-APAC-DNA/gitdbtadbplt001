with source as (
    select * from {{ source('myssdl_raw', 'sdl_my_dstrbtr_doc_type') }} where file_name not in
    ( 
      select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_dstrbtr_doc_type__duplicate_test') }}
    )
),
final as (
    select
        cust_id::varchar(8) as cust_id,
        cust_nm::varchar(80) as cust_nm,
        lvl1::varchar(40) as lvl1,
        lvl2::varchar(40) as lvl2,
        wh_id::varchar(30) as wh_id,
        doc_type::varchar(20) as doc_type,
        doc_type_desc::varchar(20) as doc_type_desc,
        cdl_dttm::varchar(255) as cdl_dttm,
        curr_dt::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        file_name::varchar(255) as file_name
    from source
)

select * from final