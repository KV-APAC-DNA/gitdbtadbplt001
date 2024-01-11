with 

source as (

    select * from {{ source('bwa_access', 'bwa_dna_material_bomlist') }}

),

final as (

    select
        bic_zbomno as zbomno,
        plant,
        material,
        component,
        createdon,
        validfrom,
        validto,
        bic_zvlfromc as zvlfromc,
        bic_zvltoi as zvltoi,
        id_credat,
        bic_zcoqty as zcoqty,
        comp_scrap,
        base_uom,
        recordmode,
        bic_zhdqty as zhdqty,
        unit,
        matl_type,
        bic_zmat_type as zmat_type,
        salesorg,
        bic_zstlan as zstlan,
        bic_zstlal as zstlal,
        bic_zstlst as zstlst,
        bic_zz_tp as zz_tp,
        bic_zz_cp as zz_cp,
        bic_zfmnge as zfmnge,
        bic_zausch as zausch,
        bic_zuposz as zuposz,
        bic_zmaktx as zmaktx,
        bic_zupmng as zupmng,
        bic_zlosbs as zlosbs,
        bic_zlosvn as zlosvn,
        bic_znlfzt as znlfzt,
        bic_zverti as zverti,
        bic_znlfzv as znlfzv,
        bic_znlfmv as znlfmv,
        bic_zzausch as zzausch,
        bic_zavoau as zavoau,
        bic_znlfzv1 as znlfzv1,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm

    from source

)

select * from final
