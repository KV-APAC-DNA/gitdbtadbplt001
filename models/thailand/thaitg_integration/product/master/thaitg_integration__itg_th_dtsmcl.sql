with sdl_mds_th_distributor_msl as (
select * from {{ source('thasdl_raw', 'sdl_mds_th_distributor_msl') }}
),
final as (
    select
    trim(barcode)::varchar(50) as barcd,
    trim(productname)::varchar(50) as prod_nm,
    trim(artypecode_code)::varchar(10) as ar_typ_cd,
    to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3')::varchar(255) as cdl_dttm,
    current_timestamp()::timestamp_ntz(9) as curr_date,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from sdl_mds_th_distributor_msl
    )
select * from final
