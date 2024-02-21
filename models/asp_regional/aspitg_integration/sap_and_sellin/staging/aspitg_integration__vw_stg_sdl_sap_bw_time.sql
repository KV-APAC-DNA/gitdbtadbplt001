--Import CTE
with source as (
    select * from {{ source('bwa_access', 'bwa_azocalday00') }}
        
),

--Logical CTE

--Final CTE
final as (
    select TO_DATE(calday, 'YYYYMMDD') as calday,
            fiscvarnt,
            weekday1,
            calweek,
            calmonth,
            calmonth2,
            calquart1,
            calquarter,
            halfyear1,
            calyear,
            fiscper,
            fiscper3::int as fiscper3,
            fiscyear,
            recordmode,
            current_timestamp()::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
        from source
        where _deleted_='F'
)

--Final select
select * from final