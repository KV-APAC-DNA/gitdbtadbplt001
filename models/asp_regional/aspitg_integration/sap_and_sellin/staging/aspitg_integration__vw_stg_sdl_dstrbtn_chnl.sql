--Import CTE
with source as (
    select * from {{ source('bwa_access', 'bwa_distribution_channel_text') }}
),

--Logical CTE

--Final CTE
final as (
    select 
        distr_chan,
        langu,
        txtsh,
        txtmd 
    from source
)

--Final select
select * from final