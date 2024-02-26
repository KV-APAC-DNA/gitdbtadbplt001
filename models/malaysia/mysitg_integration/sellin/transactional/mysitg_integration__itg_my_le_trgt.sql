with sdl_my_le_trgt as (
    select * from {{ source('myssdl_raw','sdl_my_le_trgt') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),

b as (
    select
        "year" as jj_year,
        qrtr_no,
        qrtr as jj_qtr,
        mnth_id as jj_mnth_id,
        mnth_desc,
        mnth_no as jj_mnth_no,
        mnth_shrt,
        mnth_long
    from edw_vw_os_time_dim as vt
    group by
        "year",
        qrtr_no,
        qrtr,
        mnth_id,
        mnth_desc,
        mnth_no,
        mnth_shrt,
        mnth_long
),
transformed as (
    select 
        a.cust_id,
        a.cust_nm,
        a.mnth_nm,
        b.jj_year ,
        b.mnth_shrt,
        b.jj_mnth_id,
        a.trgt_type,
        a.trgt_val_type,
        cast(a.wk1 as decimal(20, 6)) as wk1,
        cast(a.wk2 as decimal(20, 6)) as wk2,
        cast(a.wk3 as decimal(20, 6)) as wk3,
        cast(a.wk4 as decimal(20, 6)) as wk4,
        cast(a.wk5 as decimal(20, 6)) as wk5,
        a.cdl_dttm,
        a.curr_dt,
        current_timestamp()
    from sdl_my_le_trgt as a, b
    where
        a.jj_year = b.jj_year and upper(a.mnth_nm) = b.mnth_shrt
),

final as (
    select 
        cust_id::varchar(20) as cust_id,
        cust_nm::varchar(50) as cust_nm,
        jj_year::varchar(10) as jj_year,
        mnth_nm::varchar(10) as mnth_nm,
        jj_mnth_id::varchar(10) as jj_mnth_id,
        trgt_type::varchar(10) as trgt_type,
        trgt_val_type::varchar(10) as trgt_val_type,
        wk1::number(20,6) as wk1,
        wk2::number(20,6) as wk2,
        wk3::number(20,6) as wk3,
        wk4::number(20,6) as wk4,
        wk5::number(20,6) as wk5,
        cdl_dttm::varchar(20) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from transformed
)

select * from final