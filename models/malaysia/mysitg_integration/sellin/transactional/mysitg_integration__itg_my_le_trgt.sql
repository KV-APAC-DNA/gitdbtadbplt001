with sdl_my_le_trgt as (
    select * from {{ source('myssdl_raw','sdl_my_le_trgt') }}  where file_name not in
    ( select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_le_trgt__lookup_test') }}
    )
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
final as (
    select 
        a.cust_id::varchar(20) as cust_id,
        a.cust_nm::varchar(50) as cust_nm,
        b.jj_year::varchar(10) as jj_year,
        a.mnth_nm::varchar(10) as mnth_nm,
        b.jj_mnth_id::varchar(10) as jj_mnth_id,
        a.trgt_type::varchar(10) as trgt_type,
        a.trgt_val_type::varchar(10) as trgt_val_type,
        a.wk1::number(20,6) as wk1,
        a.wk2::number(20,6) as wk2,
        a.wk3::number(20,6) as wk3,
        a.wk4::number(20,6) as wk4,
        a.wk5::number(20,6) as wk5,
        a.cdl_dttm::varchar(20) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        a.file_name::varchar(255) as file_name
    from sdl_my_le_trgt as a, b
    where
        a.jj_year = b.jj_year and upper(a.mnth_nm) = b.mnth_shrt
)
select * from final