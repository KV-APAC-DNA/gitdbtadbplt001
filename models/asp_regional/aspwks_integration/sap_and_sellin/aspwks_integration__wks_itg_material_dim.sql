{{
    config(
        alias= "wks_itg_material_dim",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        tags= ["daily"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_material_dim') }}
),

--Logical CTE

--Final CTE
final as (
    select
    matl_num,
    base_uom,
    basic_matl,
    to_date(createdon, 'yyyy-mm-dd') as createdon,
    division,
    eanupc,
    gross_wt,
    logsys,
    manufactor,
    manu_matnr,
    matl_cat,
    matl_group,
    matl_type,
    net_weight,
    po_unit,
    prod_hier,
    rt_sups,
    size_dim,
    unit_dim,
    unit_of_wt,
    volume,
    volumeunit,
    height,
    lenght,
    width,
    bic_zz_mvgr1,
    bic_zz_mvgr2,
    bic_zz_mvgr3,
    bic_zz_mvgr4,
    bic_zz_mvgr5,
    prodh1,
    prodh2,
    prodh3,
    prodh4,
    prodh5,
    prodh6,
    bic_zmerciapl,
    to_date(ch_on, 'yyyy-mm-dd') as ch_on,
    createdby,
    nvl2(datefrom, null, to_date(datefrom, 'yyyy-mm-dd')) as datefrom,
    del_flag,
    ean_numtyp,
    bic_yuomcnvf,
    bic_ztragr,
    bic_zxchpf,
    bic_zpur_key,
    bic_zhaz_mat,
    bic_zstr_cond,
    bic_zmhdrz,
    bic_zmhdhb,
    bic_zqmpur,
    bic_zmstav,
    bic_zskutec,
    bic_zmjrtec,
    bic_zz_color,
    bic_zz_stack,
    bic_ztaklv,
    extmatlgrp,
    bic_zvpsta,
    bic_zpstat,
    bic_zoldmatl,
    std_descr,
    bic_zferth,
    bic_zmsource,
    bic_zbramin,
    bic_zprodseg,
    --tgt.crt_dttm as tgt_crt_dttm,
    updt_dttm
    --case when tgt.crt_dttm is null then 'i' else 'u' end as chng_flg
  from source   
)

--Final select
select * from final