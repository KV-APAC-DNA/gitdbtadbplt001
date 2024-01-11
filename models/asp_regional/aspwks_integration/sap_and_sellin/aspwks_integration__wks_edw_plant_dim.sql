with itg_plnt as (
    select * from {{ ref('aspitg_integration__itg_plnt') }}
),

itg_plnt_text as (
    select * from {{ ref('aspitg_integration__itg_plnt_text') }}
),

--Join
transformed as (
    select
        plnt,
        plnt_nm,
        ctry,
        src_sys,
        prchsng_org,
        rgn,
        co_cd,
        fctry_cal,
        mkt_clus,
        -- tgt.crt_dttm as tgt_crt_dttm,
        updt_dttm
        -- case when tgt.crt_dttm is null then 'i' else 'u' end as chng_flg
    from (select a.*, b.med_desc as plnt_nm
    from itg_plnt as a
    left outer join itg_plnt_text as b 
    on a.plnt=b.plnt)
),

final as (
    select
        plnt,
        plnt_nm,
        ctry,
        src_sys,
        prchsng_org,
        rgn,
        co_cd,
        fctry_cal,
        mkt_clus,
        updt_dttm
    from transformed
)

select * from final
