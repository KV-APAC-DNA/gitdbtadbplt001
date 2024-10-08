with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_tw_ims_dstr_std_customer_107479_customer') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_ims_dstr_std_customer_107479_customer__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_ims_dstr_std_customer_107479_customer__duplicate_test') }}
    )
),
final as
(
    select 
    distributor_code::varchar(10) as distributor_code,
	distributor_cusotmer_code::varchar(50) as distributor_cusotmer_code,
	distributor_customer_name::varchar(50) as distributor_customer_name,
	distributor_address::varchar(255) as distributor_address,
	distributor_telephone::varchar(255) as distributor_telephone,
	distributor_contact::varchar(255) as distributor_contact,
	distributor_sales_area::varchar(20) as distributor_sales_area,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    file_name::varchar(255) as file_name
    from source
)
select * from final