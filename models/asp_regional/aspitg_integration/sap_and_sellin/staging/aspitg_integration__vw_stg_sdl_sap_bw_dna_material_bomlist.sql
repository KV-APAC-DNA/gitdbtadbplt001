with 

source as (

    select * from {{ source('bwa_access', 'bwa_dna_material_bomlist') }}

),

final as (

    select
            coalesce(bic_zbomno,'') as zbomno,
            coalesce(plant,'') as plant,
            coalesce(material,'') as material,
            coalesce(component,'') as component,
            coalesce(createdon,'') as createdon,
            coalesce(validfrom,'') as validfrom,
            coalesce(validto,'') as validto,
            coalesce(bic_zvlfromc,'') as zvlfromc,
            coalesce(bic_zvltoi,'') as zvltoi,
            coalesce(id_credat,'') as id_credat,
            coalesce(bic_zcoqty,'') as zcoqty,
            coalesce(comp_scrap,'') as comp_scrap,
            coalesce(base_uom,'') as base_uom,
            coalesce(recordmode,'') as recordmode,
            coalesce(bic_zhdqty,'') as zhdqty,
            coalesce(unit,'') as unit,
            coalesce(matl_type,'') as matl_type,
            coalesce(bic_zmat_type,'') as zmat_type,
            coalesce(salesorg,'') as salesorg,
            coalesce(bic_zstlan,'') as zstlan,
            coalesce(bic_zstlal,'') as zstlal,
            coalesce(bic_zstlst,'') as zstlst,
            coalesce(bic_zz_tp,'') as zz_tp,
            coalesce(bic_zz_cp,'') as zz_cp,
            coalesce(bic_zfmnge,'') as zfmnge,
            replace(coalesce(bic_zausch,''),' ','') as zausch,
            coalesce(bic_zuposz,'') as zuposz,
            coalesce(bic_zmaktx,'') as zmaktx,
            coalesce(bic_zupmng,'') as zupmng,
            coalesce(bic_zlosbs,'') as zlosbs,
            coalesce(bic_zlosvn,'') as zlosvn,
            coalesce(bic_znlfzt,'') as znlfzt,
            coalesce(bic_zverti,'') as zverti,
            coalesce(bic_znlfzv,'') as znlfzv,
            coalesce(bic_znlfmv,'') as znlfmv,
            coalesce(bic_zzausch,'') as zzausch,
            coalesce(bic_zavoau,'') as zavoau,
            coalesce(bic_znlfzv1,'') as znlfzv1,
            current_timestamp()::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm

    from source

)

select * from final
