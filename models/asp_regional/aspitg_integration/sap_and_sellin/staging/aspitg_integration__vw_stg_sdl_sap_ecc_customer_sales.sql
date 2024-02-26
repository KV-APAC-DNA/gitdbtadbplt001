

--import CTE
with knvv as (
    select * from {{ source('apc_access', 'apc_knvv') }}
),

knvp as(
    select kunnr, vkorg, vtweg, spart,pernr from {{ source('apc_access', 'apc_knvp') }}
    where parvw = 'RG' and _deleted_='F'
group by kunnr, vkorg, vtweg, spart,pernr
),

--logical CTE
final as(
    select
        '888' as mandt,
        iff(knvv.kunnr=' ','',knvv.kunnr) as kunnr,
        iff(knvv.vkorg=' ','',knvv.vkorg) as vkorg,
        iff(knvv.vtweg=' ','',knvv.vtweg) as vtweg,
        iff(knvv.spart=' ','',knvv.spart) as spart,
        iff(knvv.ernam=' ','',knvv.ernam) as ernam,
        try_to_date(knvv.erdat,'YYYYMMDD') as erdat,
        iff(knvv.begru=' ','',knvv.begru) as begru,
        iff(knvv.loevm=' ','',knvv.loevm) as loevm,
        iff(knvv.versg=' ','',knvv.versg) as versg,
        iff(knvv.aufsd=' ','',knvv.aufsd) as aufsd,
        iff(knvv.kalks=' ','',knvv.kalks) as kalks,
        iff(knvv.kdgrp=' ','',knvv.kdgrp) as kdgrp,
        iff(knvv.bzirk=' ','',knvv.bzirk) as bzirk,
        iff(knvv.konda=' ','',knvv.konda) as konda,
        iff(knvv.pltyp=' ','',knvv.pltyp) as pltyp,
        iff(knvv.awahr=' ','',knvv.awahr) as awahr,
        iff(knvv.inco1=' ','',knvv.inco1) as inco1,
        iff(knvv.inco2=' ','',knvv.inco2) as inco2,
        iff(knvv.lifsd=' ','',knvv.lifsd) as lifsd,
        iff(knvv.autlf=' ','',knvv.autlf) as autlf,
        iff(knvv.antlf=' ','',knvv.antlf) as antlf,
        iff(knvv.kztlf=' ','',knvv.kztlf) as kztlf,
        iff(knvv.kzazu=' ','',knvv.kzazu) as kzazu,
        iff(knvv.chspl=' ','',knvv.chspl) as chspl,
        iff(knvv.lprio=' ','',knvv.lprio) as lprio,
        iff(knvv.eikto=' ','',knvv.eikto) as eikto,
        iff(knvv.vsbed=' ','',knvv.vsbed) as vsbed,
        iff(knvv.faksd=' ','',knvv.faksd) as faksd,
        iff(knvv.mrnkz=' ','',knvv.mrnkz) as mrnkz,
        iff(knvv.perfk=' ','',knvv.perfk) as perfk,
        iff(knvv.perrl=' ','',knvv.perrl) as perrl,
        iff(knvv.kvakz=' ','',knvv.kvakz) as kvakz,
        iff(knvv.kvawt=' ','',knvv.kvawt) as kvawt,
        iff(knvv.waers=' ','',knvv.waers) as waers,
        iff(knvv.klabc=' ','',knvv.klabc) as klabc,
        iff(knvv.ktgrd=' ','',knvv.ktgrd) as ktgrd,
        iff(knvv.vwerk=' ','',knvv.vwerk) as vwerk,
        iff(knvv.vkgrp=' ','',knvv.vkgrp) as vkgrp,
        iff(knvv.vkbur=' ','',knvv.vkbur) as vkbur,
        iff(knvv.vsort=' ','',knvv.vsort) as vsort,
        iff(knvv.kvgr1=' ','',knvv.kvgr1) as kvgr1,
        iff(knvv.kvgr2=' ','',knvv.kvgr2) as kvgr2,
        iff(knvv.kvgr3=' ','',knvv.kvgr3) as kvgr3,
        iff(knvv.kvgr4=' ','',knvv.kvgr4) as kvgr4,
        iff(knvv.kvgr5=' ','',knvv.kvgr5) as kvgr5,
        iff(knvv.bokre=' ','',knvv.bokre) as bokre,
        try_to_date(knvv.boidt,'YYYYMMDD') as boidt,
        iff(knvv.kurst=' ','',knvv.kurst) as kurst,
        iff(knvv.prfre=' ','',knvv.prfre) as prfre,
        iff(knvv.prat1=' ','',knvv.prat1) as prat1,
        iff(knvv.prat2=' ','',knvv.prat2) as prat2,
        iff(knvv.prat3=' ','',knvv.prat3) as prat3,
        iff(knvv.prat4=' ','',knvv.prat4) as prat4,
        iff(knvv.prat5=' ','',knvv.prat5) as prat5,
        iff(knvv.prat6=' ','',knvv.prat6) as prat6,
        iff(knvv.prat7=' ','',knvv.prat7) as prat7,
        iff(knvv.prat8=' ','',knvv.prat8) as prat8,
        iff(knvv.prat9=' ','',knvv.prat9) as prat9,
        iff(knvv.prata=' ','',knvv.prata) as prata,
        iff(knvv.zterm=' ','',knvv.zterm) as zterm,
        iff(knvp.pernr=' ','',knvp.pernr) as zzsalesrep,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
        from knvv
        left join knvp
        on knvv.kunnr = knvp.kunnr
        and knvv.vkorg = knvp.vkorg
        and knvv.vtweg = knvp.vtweg
        and knvv.spart = knvp.spart
        where _deleted_='F'
)
--final select
select * from final

