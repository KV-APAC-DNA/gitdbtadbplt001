--Import CTE
with source as (
    select * from {{ source('bwa_access', 'bwa_distribution_channel_text') }}
),

--Final CTE
final as (
    select 
        coalesce(distr_chan,'') as distr_chan,
        coalesce(langu,'') as langu,
        coalesce(txtsh,'') as txtsh,
        coalesce(txtmd,'') as txtmd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

--Final select
select * from final