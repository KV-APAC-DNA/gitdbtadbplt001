{{
   config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where fisc_year||month_nm in (select year||month from {{source('indsdl_raw','sdl_salesman_target')}});
        {% endif %}"
    )
}}
with sdl_salesman_target as
(
    select * from {{ source('indsdl_raw', 'sdl_salesman_target') }}
),

trans as 
(
    select 
        'IN' as ctry_cd,
        'INR' as crncy_cd,
        YEAR as fisc_year,
        CASE
            WHEN MONTH = 'Jan' THEN '01'
            WHEN MONTH = 'Feb' THEN '02'
            WHEN MONTH = 'Mar' THEN '03'
            WHEN MONTH = 'Apr' THEN '04'
            WHEN MONTH = 'May' THEN '05'
            WHEN MONTH = 'Jun' THEN '06'
            WHEN MONTH = 'Jul' THEN '07'
            WHEN MONTH = 'Aug' THEN '08'
            WHEN MONTH = 'Sep' THEN '09'
            WHEN MONTH = 'Oct' THEN '10'
            WHEN MONTH = 'Nov' THEN '11'
            WHEN MONTH = 'Dec' THEN '12'
            ELSE '00'
        END as fisc_mnth,
        MONTH as month_nm,
        dist_code,
        SPLIT_PART(sm_code, '-', 2) as sm_code,
        channel,
        brand_focus,
        measure_type,
        sm_target as sm_tgt_amt,
        CURRENT_TIMESTAMP() as crt_dttm,
        file_name
    from sdl_salesman_target
),

final as
(
    select
    ctry_cd::varchar(2) as ctry_cd,
	crncy_cd::varchar(3) as crncy_cd,
	fisc_year::number(18,0) as fisc_year,
	fisc_mnth::varchar(2) as fisc_mnth,
	month_nm::varchar(20) as month_nm,
	dist_code::varchar(50) as dist_code,
	sm_code::varchar(50) as sm_code,
	channel::varchar(100) as channel,
	brand_focus::varchar(50) as brand_focus,
	measure_type::varchar(50) as measure_type,
	sm_tgt_amt::number(38,6) as sm_tgt_amt,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
    file_name::varchar(225) as file_name
    
    from trans
)
select * from final