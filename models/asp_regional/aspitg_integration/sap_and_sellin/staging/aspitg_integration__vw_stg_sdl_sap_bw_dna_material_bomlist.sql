with 

source as (

    select * from {{ source('aspsdl_raw', 'sdl_sap_bw_dna_material_bomlist') }}

),

final as (

    select
        zbomno,
        plant,
        material,
        component,
        createdon,
        validfrom,
        validto,
        zvlfromc,
        zvltoi,
        id_credat,
        zcoqty,
        comp_scrap,
        base_uom,
        recordmode,
        zhdqty,
        unit,
        matl_type,
        zmat_type,
        salesorg,
        zstlan,
        zstlal,
        zstlst,
        zz_tp,
        zz_cp,
        zfmnge,
        zausch,
        zuposz,
        zmaktx,
        zupmng,
        zlosbs,
        zlosvn,
        znlfzt,
        zverti,
        znlfzv,
        znlfmv,
        zzausch,
        zavoau,
        znlfzv1,
        crt_dttm,
        updt_dttm

    from source

)

select * from final
