{{
    config(
        alias= "itg_edw_material_dim_updt",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        tags= ["daily"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__itg_material_dim') }}
),

--Logical CTE

--Final CTE
final as (
    select
        matl_num,
        base_uom,
        basic_matl,
        createdon,
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
        ch_on,
        createdby,
        datefrom,
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
        bic_zprodseg
    from source
)

--Final select
select * from final