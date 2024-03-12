

--Import CTE
with source as (
    select * from {{ source('bwa_access', 'bwa_tmaterial') }}
),

--Logical CTE

--Final CTE
final as (
    select 
            nvl(material,'') as matnr,
            langu as spras,
            txtmd as txtmd,
            current_timestamp()::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
        from source
        where langu = 'E' and _deleted_='F'
)

--Final select
select * from final