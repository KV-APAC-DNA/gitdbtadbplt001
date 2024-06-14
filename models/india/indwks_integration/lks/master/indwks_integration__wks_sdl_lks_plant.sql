with sdl_xdm_distributor_supplier as 
(
    select * from {{ source('indsdl_raw', 'sdl_xdm_distributor_supplier') }}
),
sdl_xdm_supplier as 
(
    select * from dev_dna_load.snapindsdl_raw.sdl_xdm_supplier
),
final as 
(
    select
    dist.DistrCode as plantcode
    ,substring(dist.supcode,4,4) as plantid
    ,sup.supname as plantname
    ,null as shortname
    ,null as name2
    ,sup.state as statecode
    ,null as active
    ,null as createdby 
    ,sup.SupCode as suppliercode
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm
    from sdl_xdm_distributor_supplier dist
    left join sdl_xdm_supplier sup 
    on sup.supcode = dist.supcode
    where dist.IsDefault = 'Y'
)

select * from final