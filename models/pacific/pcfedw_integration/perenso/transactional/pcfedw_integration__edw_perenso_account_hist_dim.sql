with source as(
    select * from {{ source('pcfedw_integration', 'edw_perenso_account_hist_dim') }}
),
final as(
    select * from source
)
select * from final