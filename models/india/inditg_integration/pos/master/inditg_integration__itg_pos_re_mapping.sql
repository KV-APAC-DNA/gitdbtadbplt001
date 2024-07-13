with sdl_pos_re_mapping as
(
    select * from {{ source('indsdl_raw', 'sdl_pos_re_mapping') }}
),
final as 
(
    select
        store_cd::varchar(20) as store_cd,
	    account_name::varchar(255) as account_name,
	    store_name::varchar(255) as store_name,
	    region::varchar(255) as region,
	    zone::varchar(255) as zone,
	    re::varchar(255) as re,
	    promotor::varchar(10) as promotor,
	    filename::varchar(100) as filename,
	    run_id::number(14,0) as run_id,
	    crt_dttm::timestamp_ntz(9) as crt_dttm
           
    from sdl_pos_re_mapping   
)
select * from final