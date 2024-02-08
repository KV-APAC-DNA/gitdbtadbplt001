with source as (
    select * from {{ source('myssdl_raw','sdl_my_trgts') }}
),

tbl1 as (
    select
        cust_id,
        cust_nm,
        brnd_desc,
        sub_segment,
        jj_year,
        (
            jj_year + '01'
        ) as jj_mnth_id,
        trgt_type,
        trgt_val_type,
        cast(jan_val as decimal(20, 6)) as trgt_val,
        cdl_dttm
    from source

),
tbl2 as (
    select
        cust_id,
        cust_nm,
        brnd_desc,
        sub_segment,
        jj_year,
        (
            jj_year + '02'
        ) as jj_mnth_id,
        trgt_type,
        trgt_val_type,
        cast(feb_val as decimal(20, 6)) as trgt_val,
        cdl_dttm
    from source

),
tbl3 as (
    select
        cust_id,
        cust_nm,
        brnd_desc,
        sub_segment,
        jj_year,
        (
            jj_year + '03'
        ) as jj_mnth_id,
        trgt_type,
        trgt_val_type,
        cast(mar_val as decimal(20, 6)) as trgt_val,
        cdl_dttm
    from source
),
tbl4 as (
    select
        cust_id,
        cust_nm,
        brnd_desc,
        sub_segment,
        jj_year,
        (
            jj_year + '04'
        ) as jj_mnth_id,
        trgt_type,
        trgt_val_type,
        cast(apr_val as decimal(20, 6)) as trgt_val,
        cdl_dttm
    from source
),
tbl5 as (
    select
        cust_id,
        cust_nm,
        brnd_desc,
        sub_segment,
        jj_year,
        (
            jj_year + '05'
        ) as jj_mnth_id,
        trgt_type,
        trgt_val_type,
        cast(may_val as decimal(20, 6)) as trgt_val,
        cdl_dttm
    from source
),
tbl6 as (
    select
        cust_id,
        cust_nm,
        brnd_desc,
        sub_segment,
        jj_year,
        (
            jj_year + '06'
        ) as jj_mnth_id,
        trgt_type,
        trgt_val_type,
        cast(jun_val as decimal(20, 6)) as trgt_val,
        cdl_dttm
    from source
),
tbl7 as (
    select
        cust_id,
        cust_nm,
        brnd_desc,
        sub_segment,
        jj_year,
        (
            jj_year + '07'
        ) as jj_mnth_id,
        trgt_type,
        trgt_val_type,
        cast(jul_val as decimal(20, 6)) as trgt_val,
        cdl_dttm
    from source
),
tbl8 as (
    select
        cust_id,
        cust_nm,
        brnd_desc,
        sub_segment,
        jj_year,
        (
            jj_year + '08'
        ) as jj_mnth_id,
        trgt_type,
        trgt_val_type,
        cast(aug_val as decimal(20, 6)) as trgt_val,
        cdl_dttm
    from source
),
tbl9 as (
    select
        cust_id,
        cust_nm,
        brnd_desc,
        sub_segment,
        jj_year,
        (
            jj_year + '09'
        ) as jj_mnth_id,
        trgt_type,
        trgt_val_type,
        cast(sep_val as decimal(20, 6)) as trgt_val,
        cdl_dttm
    from source
),
tbl10 as (
    select
        cust_id,
        cust_nm,
        brnd_desc,
        sub_segment,
        jj_year,
        (
            jj_year + '10'
        ) as jj_mnth_id,
        trgt_type,
        trgt_val_type,
        cast(oct_val as decimal(20, 6)) as trgt_val,
        cdl_dttm
    from source
),
tbl11 as (
    select
        cust_id,
        cust_nm,
        brnd_desc,
        sub_segment,
        jj_year,
        (
            jj_year + '11'
        ) as jj_mnth_id,
        trgt_type,
        trgt_val_type,
        cast(nov_val as decimal(20, 6)) as trgt_val,
        cdl_dttm
    from source
),
tbl12 as (
    select
        cust_id,
        cust_nm,
        brnd_desc,
        sub_segment,
        jj_year,
        (
            jj_year + '12'
        ) as jj_mnth_id,
        trgt_type,
        trgt_val_type,
        cast(dec_val as decimal(20, 6)) as trgt_val,
        cdl_dttm
    from source
),
transformed as (
    select * from tbl1 
    union all
    select * from tbl2 
    union all
    select * from tbl3 
    union all
    select * from tbl4 
    union all
    select * from tbl5 
    union all
    select * from tbl6
    union all
    select * from tbl7 
    union all
    select * from tbl8 
    union all
    select * from tbl9 
    union all
    select * from tbl10 
    union all
    select * from tbl11 
    union all
    select * from tbl12 

),

final as (
    select 
        cust_id::varchar(20) as cust_id,
        cust_nm::varchar(100) as cust_nm,
        brnd_desc::varchar(100) as brnd_desc,
        sub_segment::varchar(100) as sub_segment,
        jj_year::varchar(10) as jj_year,
        jj_mnth_id::varchar(10) as jj_mnth_id,
        trgt_type::varchar(20) as trgt_type,
        trgt_val_type::varchar(20) as trgt_val_type,
        trgt_val::number(20,6) as trgt_val,
        cdl_dttm::varchar(20) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from transformed
)

select * from final