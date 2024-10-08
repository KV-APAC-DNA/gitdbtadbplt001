{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["mandt","matnr","bwkey","bwtar"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_ecc_standard_cost') }}
),

final as (
    select
        mandt::varchar(3) as mandt,
        matnr::varchar(18) as matnr,
        bwkey::varchar(4) as bwkey,
        bwtar::varchar(10) as bwtar,
        lvorm::varchar(1) as lvorm,
        lbkum::number(13,3) as lbkum,
        salk3::number(13,2) as salk3,
        vprsv::varchar(1) as vprsv,
        verpr::number(11,2) as verpr,
        stprs::number(11,2) as stprs,
        peinh::number(5,0) as peinh,
        bklas::varchar(4) as bklas,
        salkv::number(13,2) as salkv,
        vmkum::number(13,3) as vmkum,
        vmsal::number(13,2) as vmsal,
        vmvpr::varchar(1) as vmvpr,
        vmver::number(11,2) as vmver,
        vmstp::number(11,2) as vmstp,
        vmpei::number(5,0) as vmpei,
        vmbkl::varchar(4) as vmbkl,
        vmsav::number(13,2) as vmsav,
        vjkum::number(13,3) as vjkum,
        vjsal::number(13,2) as vjsal,
        vjvpr::varchar(1) as vjvpr,
        vjver::number(11,2) as vjver,
        vjstp::number(11,2) as vjstp,
        vjpei::number(5,0) as vjpei,
        vjbkl::varchar(4) as vjbkl,
        vjsav::number(13,2) as vjsav,
        lfgja::number(4,0) as lfgja,
        lfmon::number(2,0) as lfmon,
        bwtty::varchar(1) as bwtty,
        stprv::number(11,2) as stprv,
        try_to_date(laepr) as laepr,
        zkprs::number(11,2) as zkprs,
        try_to_date(zkdat) as zkdat,
        to_timestamp(timestamps,'YYYY-MM-DD HH:MI:SS')::timestamp_ntz(9) as timestamps,
        bwprs::number(11,2) as bwprs,
        bwprh::number(11,2) as bwprh,
        vjbws::number(11,2) as vjbws,
        vjbwh::number(11,2) as vjbwh,
        vvjsl::number(13,2) as vvjsl,
        vvjlb::number(13,3) as vvjlb,
        vvmlb::number(13,3) as vvmlb,
        vvsal::number(13,2) as vvsal,
        zplpr::number(11,2) as zplpr,
        zplp1::number(11,2) as zplp1,
        zplp2::number(11,2) as zplp2,
        zplp3::number(11,2) as zplp3,
        try_to_date(zpld1) as zpld1,
        try_to_date(zpld2) as zpld2,
        try_to_date(zpld3) as zpld3,
        pperz::number(6,0) as pperz,
        pperl::number(6,0) as pperl,
        pperv::number(6,0) as pperv,
        kalkz::varchar(1) as kalkz,
        kalkl::varchar(1) as kalkl,
        kalkv::varchar(1) as kalkv,
        kalsc::varchar(6) as kalsc,
        xlifo::varchar(1) as xlifo,
        mypol::varchar(4) as mypol,
        bwph1::number(11,2) as bwph1,
        bwps1::number(11,2) as bwps1,
        abwkz::number(2,0) as abwkz,
        pstat::varchar(15) as pstat,
        kaln1::number(12,0) as kaln1,
        kalnr::number(12,0) as kalnr,
        bwva1::varchar(3) as bwva1,
        bwva2::varchar(3) as bwva2,
        bwva3::varchar(3) as bwva3,
        vers1::number(2,0) as vers1,
        vers2::number(2,0) as vers2,
        vers3::number(2,0) as vers3,
        hrkft::varchar(4) as hrkft,
        kosgr::varchar(10) as kosgr,
        pprdz::number(3,0) as pprdz,
        pprdl::number(3,0) as pprdl,
        pprdv::number(3,0) as pprdv,
        pdatz::number(4,0) as pdatz,
        pdatl::number(4,0) as pdatl,
        pdatv::number(4,0) as pdatv,
        ekalr::varchar(1) as ekalr,
        vplpr::number(11,2) as vplpr,
        mlmaa::varchar(1) as mlmaa,
        mlast::varchar(1) as mlast,
        lplpr::number(11,2) as lplpr,
        vksal::number(13,2) as vksal,
        hkmat::varchar(1) as hkmat,
        sperw::varchar(1) as sperw,
        kziwl::varchar(3) as kziwl,
        try_to_date(wlinl) as wlinl,
        abciw::varchar(1) as abciw,
        bwspa::number(6,2) as bwspa,
        lplpx::number(11,2) as lplpx,
        vplpx::number(11,2) as vplpx,
        fplpx::number(11,2) as fplpx,
        lbwst::varchar(1) as lbwst,
        vbwst::varchar(1) as vbwst,
        fbwst::varchar(1) as fbwst,
        eklas::varchar(4) as eklas,
        qklas::varchar(4) as qklas,
        mtuse::varchar(1) as mtuse,
        mtorg::varchar(1) as mtorg,
        ownpr::varchar(1) as ownpr,
        xbewm::varchar(1) as xbewm,
        bwpei::number(5,0) as bwpei,
        mbrue::varchar(1) as mbrue,
        oklas::varchar(4) as oklas,
        oippinv::varchar(1) as oippinv,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final