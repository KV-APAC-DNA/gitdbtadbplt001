with source as
(
    -- select * from {{ ref('ntawks_integration__wks_sales_rep_so_target_fact')}}
    select * from dev_dna_core.snapntawks_integration.wks_sales_rep_so_target_fact
),

t2 as
(
    select
        ctry_cd,
        dstr_cd,
        jj_mnth_id,
        max (file_rec_dt) as file_rec_dt
    from source
    group by ctry_cd, dstr_cd, jj_mnth_id
),

final as
(
    select
        t1.ctry_cd,
        t1.crncy_cd,
        t1.dstr_cd,
        t1.jj_mnth_id,
        t1.sls_rep_cd,
        t1.sls_rep_nm,
        t1.brand,
        trunc(t1.sls_trgt_val,5) as sls_trgt_val,
        t2.file_rec_dt,
        cast(null as date)
    from source t1, t2
    WHERE t1.file_rec_dt = t2.file_rec_dt
        AND t1.jj_mnth_id = t2.jj_mnth_id
        AND t1.ctry_cd = t2.ctry_cd
        AND t1.dstr_cd = t2.dstr_cd
)

select * from final