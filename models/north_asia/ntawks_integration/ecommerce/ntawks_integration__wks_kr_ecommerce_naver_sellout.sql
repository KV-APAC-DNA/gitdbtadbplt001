with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_ecom_naver_sellout_temp') }}
),
v_calendar_promo_univ_dtls as (
    select * from {{ ref ('aspedw_integration__v_calendar_promo_univ_dtls') }}
),
trnsctn_dt_not_null as (
    select 'NAVER' as cust_nm,
        null as cust_code,
        null as sub_cust_nm,
        ean as ean_num,
        brand_name as brand,
        product_name as prod_desc,
        year,
        month,
        case
            when UPPER(week) = 'W1' then '1'
            when UPPER(week) = 'W2' then '2'
            when UPPER(week) = 'W3' then '3'
            when UPPER(week) = 'W4' then '4'
            when UPPER(week) = 'W5' then '5'
        end as week,
        transaction_date,
        sales_qty as sellout_qty,
        sales_amount as sellout_amount,
        current_timestamp() as crtd_dttm,
        file_name as file_name,
        run_id as run_id
    from source
    where (
            transaction_date is not null
            or transaction_date != ''
        )
        and UPPER(brand_name) != 'GIFT'
),
modified as (
    select 'NAVER' as cust_nm,
        null as cust_code,
        null as sub_cust_nm,
        ean as ean_num,
        brand_name as brand,
        product_name as prod_desc,
        year,
        case
            when month = 'Jan' then '01'
            when month = 'Feb' then '02'
            when month = 'Mar' then '03'
            when month = 'Apr' then '04'
            when month = 'May' then '05'
            when month = 'Jun' then '06'
            when month = 'Jul' then '07'
            when month = 'Aug' then '08'
            when month = 'Sep' then '09'
            when month = 'Oct' then '10'
            when month = 'Nov' then '11'
            when month = 'Dec' then '12'
        end as monthno,
        case
            when UPPER(week) = 'W1' then 1
            when UPPER(week) = 'W2' then 2
            when UPPER(week) = 'W3' then 3
            when UPPER(week) = 'W4' then 4
            when UPPER(week) = 'W5' then 5
        end as weekint,
        day,
        transaction_date,
        sales_qty as sellout_qty,
        sales_amount as sellout_amount,
        current_timestamp() as crtd_dttm,
        file_name as file_name,
        run_id as run_id
    from source
    where (
            transaction_date is null
            or transaction_date = ''
        )
        and UPPER(brand_name) != 'GIFT'
),
trnsctn_dt_null as (
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
    from modified sknav
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
transformed as (
    select * from trnsctn_dt_not_null
    union all
    select * from trnsctn_dt_null
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