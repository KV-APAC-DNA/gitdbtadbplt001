--Import CTE
with source as (
    select * from {{ source('bwa_access', 'bwa_tzneed') }}
),

--Logical CTE

--Final CTE
final as (
    select 
        bic_zneed as zneed,
        langu as langu,
        txtsh as txtsh,
        txtmd as txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
        from source
        where langu = 'E' and _deleted_='F'
)

--Final select
select * from final
