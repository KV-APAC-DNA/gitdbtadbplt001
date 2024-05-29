with source as (
    select * from dev_dna_load.ntasdl_raw.sdl_kr_ecom_naver_sellout_temp
    -- select * from {{ source('ntasdl_raw', 'sdl_kr_ecom_naver_sellout_temp') }}
),
v_calendar_promo_univ_dtls as (
    select * from aspedw_integration.v_calendar_promo_univ_dtls
),
transformed as (
    select 
        'NAVER' as cust_nm,
        null as cust_code,
        null as sub_cust_nm,
        ean_num,
        brand,
        prod_desc,
        year,
        monthno as month,
        weekint as week,
        nvl(cast(cal_day as varchar(20)), year || '-' || monthno || '-' || '15') as transaction_date,
        sellout_qty,
        sellout_amount,
        current_timestamp() as crtd_dttm,
        file_name as file_name,
        run_id as run_id
    from source sknav
        left outer join v_calendar_promo_univ_dtls cal on cast(sknav.year as integer) = cal.fisc_yr
        and weekint = cast(cal.univ_week_month as integer)
        and cast(sknav.monthno as integer) = cal.pstng_per
        and case
            when sknav.day = 'Mon' then 1
            when sknav.day = 'Tue' then 2
            when sknav.day = 'Wed' then 3
            when sknav.day = 'Thu' then 4
            when sknav.day = 'Fri' then 5
            when sknav.day = 'Sat' then 6
            when sknav.day = 'Sun' then 7
        end = cal.wkday
),
final as (
    select
        cust_nm::varchar(255) as cust_nm,
        cust_code::varchar(20) as cust_code,
        sub_cust_nm::varchar(255) as sub_cust_nm,
        ean_num::varchar(50) as ean_num,
        brand::varchar(100) as brand,
        prod_desc::varchar(255) as prod_desc,
        year::varchar(20) as year,
        month::varchar(20) as month,
        week::varchar(20) as week,
        transaction_date::varchar(50) as transaction_date,
        sellout_qty::number(20,4) as sellout_qty,
        sellout_amount::number(20,4) as sellout_amount,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        file_name::varchar(255) as file_name,
        run_id::varchar(40) as run_id
    from transformed
)
select * from final