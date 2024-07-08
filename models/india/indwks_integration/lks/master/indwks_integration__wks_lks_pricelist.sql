with itg_xdm_batchmaster as 
(
    select * from {{ ref('inditg_integration__itg_xdm_batchmaster') }}
),
sdl_xdm_product as 
(
    select * from {{ source('indsdl_raw', 'sdl_xdm_product') }}
),
final as 
(
    select
    bat.ProdCode as productcode
    ,prd.productname
    ,null as pack
    ,bat.MRP as mrpperpack
    ,null as billingunit
    ,bat.MRP as mrpperunit
    ,null as zdst
    ,null as retailermarginpercent
    ,null as retailermarginamt
    ,bat.SellRate as retailerprice
    ,null as td
    ,null as excisedutyper
    ,null as vatrds2ret
    ,null as vatrds2retamt
    ,bat.SellRate as baseprice
    ,null as tdamt
    ,null as vatjnj2rds
    ,null as vatjnj2rdsamt
    ,bat.LSP as listprice
    ,null as cd
    ,null as excisedutyamt
    ,null as excisecessamt
    ,null as excisedutytot
    ,bat.NetRate as nr
    ,null as state
    ,null as startdate
    ,null as enddate
    ,null as insertedon
    ,CAST(bat.StateCOde AS INTEGER) as statecode
    ,null as userid
    ,null as cststate
    ,null as calculateflag
    ,null as filecode

    from itg_xdm_batchmaster bat
    left join sdl_xdm_product prd
    on bat.prodcode = prd.productcode
)

select * from final