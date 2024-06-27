with source as (
    select * from {{ ref('ntaedw_integration__edw_vw_sls_rep_dim') }}
),
final as (
    select
        ctry_cd::varchar(5) as ctry_cd,
        dstr_cd::varchar(10) as dstr_cd,
        dstr_nm::varchar(100) as dstr_nm,
        sls_rep_cd::varchar(20) as sls_rep_cd,
        sls_rep_nm::varchar(50) as sls_rep_nm,
        sls_rep_typ::varchar(20) as sls_rep_typ
    from source
)
select * from final