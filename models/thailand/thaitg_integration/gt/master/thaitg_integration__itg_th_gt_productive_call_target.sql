with source as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_gt_productive_call_target') }}
),
transformed as(
    select
        'TH' as cntry_cd,
        'THB' as crncy_cd,
        upper(trim(saleunit)) as saleunit,
        upper(trim(saleid)) as saleid,
        lpad(trim(month), 2, 0) as month,
        trim(year) as year,
        trim(product) as product,
        cast(trim(target) as decimal(18, 6)) as target,
        to_char(current_timestamp()::timestampntz(9), 'YYYYMMDDHH24MISSFF3')::varchar(100) as cdl_dttm,
        current_timestamp()::timestampntz(9) as crtd_dttm,
        current_timestamp()::timestampntz(9) as updt_dttm
    from source
),
final as(
    select
        cntry_cd::varchar(5) as cntry_cd,
        crncy_cd::varchar(5) as crncy_cd,
        saleunit::varchar(50) as saleunit,
        saleid::varchar(50) as saleid,
        month::varchar(10) as month,
        year::varchar(10) as year,
        product::varchar(50) as product,
        target::number(18,6) as target,
        cdl_dttm::varchar(100) as cdl_dttm,
        crtd_dttm as crtd_dttm,
        updt_dttm as updt_dttm
    from transformed
)
select * from final