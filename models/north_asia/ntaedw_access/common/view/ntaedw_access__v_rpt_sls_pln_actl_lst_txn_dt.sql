with source as (
    select * from {{ ref('ntaedw_integration__v_rpt_sls_pln_actl_lst_txn_dt') }}
),
final as (
    select
        country as "country",
        last_txn_dt as "last_txn_dt"
    from source
)
select * from final