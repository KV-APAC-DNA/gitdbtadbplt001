with 

source as (

    select * from {{ source('bwa_access', 'bwa_account_text') }}

),

without_pipe as (

    select
        chrt_accts,
        account,
        langu,
        replace(trim(txtsh),'–','') as txtsh,
        replace(trim(txtmd),'–','') as txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where chrt_accts not like '%|%'
),
with_pipe as (
    select
        trim(split(chrt_accts,'|')[0],'"') as chrt_accts,
        trim(split(chrt_accts,'|')[1],'"') as account,
        trim(split(chrt_accts,'|')[2],'"') as langu, 
        replace(trim(trim(split(chrt_accts,'|')[3],'"')),'–','') as txtsh,
        replace(trim(trim(split(chrt_accts,'|')[4],'"')),'–','') as txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where chrt_accts like '%|%'
),
final as (
select * from without_pipe
union all 
select * from with_pipe
)

select * from final

