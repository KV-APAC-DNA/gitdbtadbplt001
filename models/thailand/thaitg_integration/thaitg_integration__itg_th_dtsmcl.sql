with sdl_mds_th_distributor_msl as (
select * from dev_dna_load.snaposesdl_raw.sdl_mds_th_distributor_msl
),
transformed as (
    select
    trim(barcode) as barcd,
    trim(productname) as prod_nm,
    trim(artypecode_code) as ar_typ_cd,
    to_char(cast(current_timestamp() as timestampntz), 'YYYYMMDDHH24MISSFF3') as cdl_dttm,
    current_timestamp() as curr_date,
    current_timestamp() as updt_dttm
    from sdl_mds_th_distributor_msl
    ),
final as (
    select 
    barcd::varchar(50) as barcd,
    prod_nm::varchar(50) as prod_nm,
    ar_typ_cd::varchar(10) as ar_typ_cd,
    cdl_dttm::varchar(255) as cdl_dttm,
    curr_date::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final
