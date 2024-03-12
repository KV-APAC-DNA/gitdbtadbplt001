with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_jnj_consumerreach_711') }}
),

final as
(
    select
        'TH'::varchar(10) as cntry_cd,
        
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final