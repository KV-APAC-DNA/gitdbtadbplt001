with source as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_npi') }}
),
final as(
    select
        trim(code)::varchar(25) as prod_cd,
        trim(name)::varchar(50) as prod_nm,
        to_date(startdate) as startdate,
        to_date(enddate) as enddate,
        null as updt_dt,
        to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3')::VARCHAR(255) as cdl_dttm,
        current_timestamp() as crtd_dttm,
        current_timestamp() as updt_dttm
    from source
)
select * from final