{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}

with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_dna_material_bomlist') }}
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
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source

)

select * from final
