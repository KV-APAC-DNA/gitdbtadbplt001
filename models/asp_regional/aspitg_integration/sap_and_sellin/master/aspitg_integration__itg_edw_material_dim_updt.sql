
--Import CTE
with source as (
    select * from {{ ref('aspitg_integration__itg_material_dim') }}
),

--Logical CTE

--Final CTE
final as (
    select
        matl_num::varchar(18) as matl_num,
        base_uom::varchar(3) as base_uom,
        basic_matl::varchar(14) as basic_matl,
        createdon::date as createdon,
        division::varchar(2) as division,
        eanupc::varchar(18) as eanupc,
        gross_wt::number(17,0) as gross_wt,
        logsys::varchar(10) as logsys,
        manufactor::varchar(10) as manufactor,
        manu_matnr::varchar(40) as manu_matnr,
        matl_cat::varchar(2) as matl_cat,
        matl_group::varchar(9) as matl_group,
        matl_type::varchar(256) as matl_type,
        net_weight::number(17,0) as net_weight,
        po_unit::varchar(3) as po_unit,
        prod_hier::varchar(18) as prod_hier,
        rt_sups::varchar(1) as rt_sups,
        size_dim::varchar(32) as size_dim,
        unit_dim::varchar(3) as unit_dim,
        unit_of_wt::varchar(10) as unit_of_wt,
        volume::number(17,0) as volume,
        volumeunit::varchar(10) as volumeunit,
        height::number(17,0) as height,
        lenght::number(17,0) as lenght,
        width::number(17,0) as width,
        bic_zz_mvgr1::varchar(3) as bic_zz_mvgr1,
        bic_zz_mvgr2::varchar(3) as bic_zz_mvgr2,
        bic_zz_mvgr3::varchar(3) as bic_zz_mvgr3,
        bic_zz_mvgr4::varchar(3) as bic_zz_mvgr4,
        bic_zz_mvgr5::varchar(3) as bic_zz_mvgr5,
        prodh1::varchar(18) as prodh1,
        prodh2::varchar(18) as prodh2,
        prodh3::varchar(18) as prodh3,
        prodh4::varchar(18) as prodh4,
        prodh5::varchar(18) as prodh5,
        prodh6::varchar(18) as prodh6,
        bic_zmerciapl::varchar(10) as bic_zmerciapl,
        ch_on::date as ch_on,
        createdby::varchar(100) as createdby,
        datefrom::date as datefrom,
        del_flag::varchar(1) as del_flag,
        ean_numtyp::varchar(10) as ean_numtyp,
        bic_yuomcnvf::number(17,0) as bic_yuomcnvf,
        bic_ztragr::varchar(4) as bic_ztragr,
        bic_zxchpf::varchar(1) as bic_zxchpf,
        bic_zpur_key::varchar(4) as bic_zpur_key,
        bic_zhaz_mat::varchar(18) as bic_zhaz_mat,
        bic_zstr_cond::varchar(2) as bic_zstr_cond,
        bic_zmhdrz::varchar(4) as bic_zmhdrz,
        bic_zmhdhb::varchar(4) as bic_zmhdhb,
        bic_zqmpur::varchar(1) as bic_zqmpur,
        bic_zmstav::varchar(2) as bic_zmstav,
        bic_zskutec::varchar(3) as bic_zskutec,
        bic_zmjrtec::varchar(35) as bic_zmjrtec,
        bic_zz_color::number(2,0) as bic_zz_color,
        bic_zz_stack::number(2,0) as bic_zz_stack,
        bic_ztaklv::varchar(1) as bic_ztaklv,
        extmatlgrp::varchar(18) as extmatlgrp,
        bic_zvpsta::varchar(15) as bic_zvpsta,
        bic_zpstat::varchar(15) as bic_zpstat,
        bic_zoldmatl::varchar(18) as bic_zoldmatl,
        std_descr::varchar(20) as std_descr,
        bic_zferth::varchar(18) as bic_zferth,
        bic_zmsource::varchar(10) as bic_zmsource,
        bic_zbramin::varchar(6) as bic_zbramin,
        bic_zprodseg::varchar(10) as bic_zprodseg
    from source
)

--Final select
select * from final