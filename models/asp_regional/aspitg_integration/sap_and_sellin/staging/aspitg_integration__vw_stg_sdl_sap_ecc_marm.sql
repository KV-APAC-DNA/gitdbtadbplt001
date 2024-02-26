
with 

source as (

    select * from {{ source('apc_access', 'apc_marm') }}

),

final as (

    select 
        iff(matnr=' ','',matnr) as matnr,
        iff(meinh=' ','',meinh) as meinh,
        iff(umrez=' ','',umrez) as umrez,
        iff(umren=' ','',umren) as umren,
        iff(eannr=' ','',eannr) as eannr,
        iff(ean11=' ','',ean11) as ean11,
        iff(numtp=' ','',numtp) as numtp,
        iff(laeng=' ','',laeng) as laeng,
        iff(breit=' ','',breit) as breit,
        iff(hoehe=' ','',hoehe) as hoehe,
        iff(meabm=' ','',meabm) as meabm,
        iff(volum=' ','',volum) as volum,
        iff(voleh=' ','',voleh) as voleh,
        iff(brgew=' ','',brgew) as brgew,
        iff(gewei=' ','',gewei) as gewei,
        iff(mesub=' ','',mesub) as mesub,
        iff(atinn=' ','',atinn) as atinn,
        iff(mesrt=' ','',mesrt) as mesrt,
        iff(xfhdw=' ','',xfhdw) as xfhdw,
        iff(xbeww=' ','',xbeww) as xbeww,
        iff(kzwso=' ','',kzwso) as kzwso,
        iff(msehi=' ','',msehi) as msehi,
        iff(bflme_marm=' ','',bflme_marm) as bflme_marm,
        iff(gtin_variant=' ','',gtin_variant) as gtin_variant,
        iff(nest_ftr=' ','',nest_ftr) as nest_ftr,
        iff(max_stack=' ','',max_stack) as max_stack,
        iff(capause=' ','',capause) as capause,
        iff(ty2tq=' ','',ty2tq) as ty2tq,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where _deleted_='F' and mandt='888'

)

select * from final
